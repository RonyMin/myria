/**
 *
 */
package edu.washington.escience.myria.operator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.MyriaConstants;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.Type;
import edu.washington.escience.myria.column.Column;
import edu.washington.escience.myria.storage.TupleBatch;
import edu.washington.escience.myria.storage.TupleBatchBuffer;

/**
 * generic operator for applying a Python based UDF
 */
public class PyUDF extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  private String filename;
  // // this is temporarily the pickled object that is sent back
  public static final Schema SCHEMA = Schema.ofFields(Type.BYTES_TYPE, "UDF");

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PyUDF.class);

  private ServerSocket serverSocket = null;
  private Socket clientSock = null;
  private Process worker = null;
  private int[] columnIdx = null;
  private DataOutputStream dOut;
  private DataInputStream dIn;
  private byte[] pyCode;
  private String pythonPath;

  /**
   * The buffer holding the results.
   */
  // private transient TupleBatchBuffer ans;

  /**
   * Evaluator that evaluates {@link #predicate}.
   */
  // private BooleanEvaluator evaluator;

  /**
   * 
   * @param child child operator that data is fetched from
   * @param emitExpressions expression that created the output
   */
  public PyUDF(final String filename, final int[] columnIdx, final Operator child) {
    super(child);
    if (null != filename) {
      this.filename = filename;
      this.columnIdx = columnIdx;
      // setup python path
      StringBuilder sb = new StringBuilder();
      sb.append(System.getenv("HOME"));
      sb.append("/anaconda/bin");
      pythonPath = sb.toString();

    }

  }

  @Override
  protected TupleBatch fetchNextReady() throws DbException, UnknownHostException, IOException {
    // this function should get the the batch of the tuples
    // apply run the python script on this batch of tuples
    // since python UDF will be applied to each tuple at a time

    Operator child = getChild();

    if (child.eoi() || child.eos()) {
      return null;
    }

    TupleBatch tb = child.nextReady();
    if (tb == null) {
      return null;
    }

    int rows = tb.numTuples();
    // read schema from output to create it ..
    final TupleBatchBuffer output = new TupleBatchBuffer(SCHEMA);
    if (clientSock != null && dOut != null && dIn != null) {

      for (int i = 0; i < rows; i++) {
        // write to stream
        try {
          // then write all the tuples to the stream in this batch
          writeToStream(tb, i);
          // LOGGER.info("wrote tuple to stream");
          // temporarily write crap back
          // sendErrorbuffer(output);
          // LOGGER.info("wrote to the stream");

          readFromStream(output);
          // LOGGER.info("successfully read from the stream");
        } catch (DbException e) {
          LOGGER.info("Error writing to python stream" + e.getMessage());
          sendErrorbuffer(output);

        }
      }

    }

    return output.popAnyUsingTimeout();

  }

  private void readFromStream(final TupleBatchBuffer output) throws DbException {
    int length = 0;

    try {
      // LOGGER.info("starting to read int from python process ");
      length = dIn.readInt(); // read length of incoming message
      // LOGGER.info("length of message to be read from python process " + length);
      switch (length) {
        case -2:
          int excepLength = dIn.readInt();
          byte[] excp = new byte[excepLength];
          dIn.readFully(excp);
          throw new DbException(new String(excp));

        default:
          if (length > 0) {
            // LOGGER.info("length > 0");
            byte[] obj = new byte[length];
            dIn.readFully(obj);
            output.putByteBuffer(0, ByteBuffer.wrap(obj));
            // LOGGER.info("read bytes from python process " + length);
          }
          break;

      }

    } catch (Exception e) {
      LOGGER.info("Error reading int from stream");
      throw new DbException(e);
    }

  }

  private void sendErrorbuffer(final TupleBatchBuffer output) {
    byte[] b = "1".getBytes();

    output.putByteBuffer(0, ByteBuffer.wrap(b));

  }

  // open a server socket for operator
  private void createServerSock() throws UnknownHostException, IOException {

    serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
    int a = serverSocket.getLocalPort();
    LOGGER.info("created socket " + a);

  }

  private void getPyCode() throws IOException {
    // this function creates a py process
    // connects to this server socket
    // uses the client socket to gets the py code
    // closes the client socket

    ProcessBuilder pb = new ProcessBuilder(MyriaConstants.DEFAULT_PYTHONPATH, "-m", filename);
    final Map<String, String> env = pb.environment();

    // add python process to path
    StringBuilder sb = new StringBuilder();
    sb.append(pythonPath);
    sb.append(":");
    sb.append(env.get("PATH"));
    env.put("PATH", sb.toString());

    pb.redirectError(Redirect.INHERIT);
    pb.redirectOutput(Redirect.INHERIT);

    Process codeReader = pb.start();

    OutputStream stdin = codeReader.getOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(stdin, StandardCharsets.UTF_8);

    out.write(serverSocket.getLocalPort() + "\n");
    out.flush();

    Socket tmpClientSock = serverSocket.accept();
    setCode(tmpClientSock);

    if (tmpClientSock != null) {
      tmpClientSock.close();
    }

    if (codeReader != null) {
      codeReader.destroy();
    }

  }

  private void startPyRunner() throws IOException {
    // this function creates a py process
    // connects to this server socket
    // sets the process as worker for the operator
    // creates streams ( input and output) for the operator
    // TODO : fix the worker filename -- it should be a myria constant and installed as part of setup.

    String workerfilename = "MyriaPythonUDF.worker";
    ProcessBuilder pb = new ProcessBuilder(MyriaConstants.DEFAULT_PYTHONPATH, "-m", workerfilename);
    final Map<String, String> env = pb.environment();

    StringBuilder sb = new StringBuilder();
    sb.append(pythonPath);
    sb.append(":");
    sb.append(env.get("PATH"));
    env.put("PATH", sb.toString());

    // String pythonPath = createPythonPath(env.get(MyriaConstants.PYTHONPATH), pythonModulePath);
    env.put("PYTHONUNBUFFERED", "YES");

    pb.redirectError(Redirect.INHERIT);
    pb.redirectOutput(Redirect.INHERIT);

    // write the env variables to the path of the starting process
    worker = pb.start();

    OutputStream stdin = worker.getOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(stdin, StandardCharsets.UTF_8);

    out.write(serverSocket.getLocalPort() + "\n");
    out.flush();
    clientSock = serverSocket.accept();
    // LOGGER.info("successfully launched worker");
    setupStreams();

    return;

  }

  private void setupStreams() throws IOException {
    if (clientSock != null) {
      dOut = new DataOutputStream(clientSock.getOutputStream());
      dIn = new DataInputStream(clientSock.getInputStream());
      // LOGGER.info("successfully setup streams");
    }

  }

  // stopPyRunner
  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

    Preconditions.checkNotNull(filename);
    // start worker
    try {
      createServerSock();

      getPyCode();
      startPyRunner();

      if (pyCode.length > 0 && dOut != null) {
        dOut.writeInt(pyCode.length);
        // LOGGER.info("length of code buffer " + pyCode.length);

        dOut.write(pyCode);
        dOut.writeInt(columnIdx.length);

        dOut.flush();
        // LOGGER.info("wrote and flushed code snippet ");

      } else {
        LOGGER.info("something is very wrong, python code  or output stream are empty");
      }

    } catch (IOException e) {
      LOGGER.info("Exception Initializing Python process on worker");
    }
  }

  private void setCode(final Socket codeSock) {
    try {
      // DataOutputStream tOut = new DataOutputStream(codeSock.getOutputStream());
      DataInputStream tIn = new DataInputStream(codeSock.getInputStream());
      int length = 0;
      length = tIn.readInt(); // read length of incoming message
      if (length > 0) {
        pyCode = new byte[length];
        tIn.read(pyCode);
      } else {
        LOGGER.info("read zero length code block");
      }

    } catch (IOException e) {
      // TODO Auto-generated catch block
      LOGGER.info(e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Do the clean up, release resources.
   * 
   * @throws Exception if any error occurs
   */
  @Override
  protected void cleanup() throws Exception {

    LOGGER.info("Operator cleanup : closing the server and client sockets.");
    // stop the writer thread

    // close sockets after operator is done.
    if (clientSock != null) {
      clientSock.close();
    }

    if (serverSocket != null) {
      serverSocket.close();
    }
    // stop worker process
    if (worker != null) {
      worker.destroy();
    }

  }

  @Override
  public Schema generateSchema() {
    return SCHEMA;
  }

  private void writeToStream(final TupleBatch tb, final int row) throws DbException {
    List<? extends Column<?>> inputColumns = tb.getDataColumns();

    if (columnIdx.length > 0) {

      try {

        for (int element : columnIdx) {

          Type type = inputColumns.get(element).getType();
          switch (type) {
            case BOOLEAN_TYPE:
              dOut.writeInt(Integer.SIZE / Byte.SIZE);
              dOut.writeBoolean(inputColumns.get(element).getBoolean(row));
              break;
            case DOUBLE_TYPE:
              dOut.writeInt(Double.SIZE / Byte.SIZE);
              dOut.writeDouble(inputColumns.get(element).getDouble(row));
              break;
            case FLOAT_TYPE:
              dOut.writeInt(Float.SIZE / Byte.SIZE);
              dOut.writeFloat(inputColumns.get(element).getFloat(row));
              break;
            case INT_TYPE:
              dOut.writeInt(Integer.SIZE / Byte.SIZE);
              dOut.writeInt(inputColumns.get(element).getInt(row));
              break;
            case LONG_TYPE:
              dOut.writeInt(Long.SIZE / Byte.SIZE);
              dOut.writeLong(inputColumns.get(element).getLong(row));
              break;
            case STRING_TYPE:
              StringBuilder sb = new StringBuilder();
              sb.append(inputColumns.get(element).getString(row));
              dOut.writeInt(sb.length());
              dOut.writeChars(sb.toString());
              break;
            case DATETIME_TYPE:
              LOGGER.info("date time not yet supported for python UDF ");
              break;
            case BYTES_TYPE:
              ByteBuffer input = inputColumns.get(element).getByteBuffer(row);
              // byte[] b = "empty".getBytes();
              // LOGGER.info("about to write to stream, length of buffer " + input.array().length);
              dOut.writeInt(input.array().length);
              // dOut.writeInt(b.length);
              // LOGGER.info("length of stream " + dOut.size());
              // LOGGER.info("length of buffer " + input.array().length);
              dOut.write(input.array());
              // dOut.write(b);
              // LOGGER.info("wrote buffer to stream, buffer length" + input.array().length);
              break;
          }
          dOut.flush();
          // LOGGER.info("flushed output buffer ");

        }

      } catch (IOException e) {
        // LOGGER.info("IOException when writing to python process stream");
        throw new DbException(e);
      }

    }
  }

}
