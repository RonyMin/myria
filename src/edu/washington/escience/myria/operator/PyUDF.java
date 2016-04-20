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

      // this is where the pycode should be initialized later
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
          writeToStream(tb, i);
          LOGGER.info("wrote to the stream");

          readFromStream(tb, i, output);
          LOGGER.info("successfully read from the stream");
        } catch (DbException e) {
          LOGGER.info("Error writing to python stream" + e.getMessage());
          sendErrorbuffer(output);

        }
      }

    }

    return output.popAnyUsingTimeout();

  }

  private void readFromStream(final TupleBatch tb, final int row, final TupleBatchBuffer output) throws DbException {
    int length = 0;

    try {
      LOGGER.info("starting to read int from python process " + length);
      length = dIn.readInt(); // read length of incoming message
      LOGGER.info("length of message to be read from python process" + length);
      if (length > 0) {
        byte[] b = new byte[length];
        dIn.read(b);
        output.putByteBuffer(0, ByteBuffer.wrap(b));
        LOGGER.info("read bytes from python process " + length);

      }
    } catch (Exception e) {
      length = 0;
      LOGGER.info("Error reading int from stream");
      throw new DbException(e);
    }

  }

  private void sendErrorbuffer(final TupleBatchBuffer output) {
    byte[] b = "empty".getBytes();

    output.putByteBuffer(0, ByteBuffer.wrap(b));
    // LOGGER.info("Failed launching worker");
  }

  private void Startworker(final String pythonExecPath) throws UnknownHostException, IOException {

    serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
    int a = serverSocket.getLocalPort();
    LOGGER.info("created socket " + a);
    // TODO : fix the worker filename -- it should be a myria constant and installed as part of setup.

    String workerfilename = "MyriaPythonUDF.worker";
    ProcessBuilder pb = new ProcessBuilder(MyriaConstants.DEFAULT_PYTHONPATH, "-m", workerfilename);
    final Map<String, String> env = pb.environment();

    String path = env.get("PATH");
    StringBuilder sb = new StringBuilder();
    sb.append(pythonExecPath);
    sb.append(":");
    sb.append(path);
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

    return;

  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

    Preconditions.checkNotNull(filename);
    // start worker
    try {

      String home = System.getenv("HOME");
      // LOGGER.info("home of current process" + home);

      String pythonpath = "/anaconda/bin";

      StringBuilder sb = new StringBuilder();
      sb.append(home);
      sb.append(pythonpath);

      getPyCode(sb.toString());
      Startworker(sb.toString());
      if (clientSock != null) {
        LOGGER.info("successfully launched worker");

        dOut = new DataOutputStream(clientSock.getOutputStream());
        dIn = new DataInputStream(clientSock.getInputStream());

        // write code to the client socket

        if (pyCode.length > 0 && dOut != null) {
          dOut.writeInt(pyCode.length);
          LOGGER.info("length of code buffer " + pyCode.length);

          dOut.write(pyCode);

          dOut.flush();
          LOGGER.info("wrote and flushed code snippet ");

        } else {
          LOGGER.info("something is very wrong, python code  or output stream are empty");
        }

      } else {
        LOGGER.info("could not launch worker");
      }

    } catch (IOException e) {
      LOGGER.info("Exception Initializing Python process on worker");
    }
  }

  private void getPyCode(final String pythonExecPath) throws UnknownHostException, IOException {
    try {

      ServerSocket tmpSSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));

      int a = tmpSSocket.getLocalPort();
      LOGGER.info("created temp Sock for reading code socket " + a);

      ProcessBuilder pb = new ProcessBuilder(MyriaConstants.DEFAULT_PYTHONPATH, "-m", filename);
      final Map<String, String> env = pb.environment();

      String path = env.get("PATH");
      StringBuilder sb = new StringBuilder();
      sb.append(pythonExecPath);
      sb.append(":");
      sb.append(path);
      env.put("PATH", sb.toString());
      // String pythonPath = createPythonPath(env.get(MyriaConstants.PYTHONPATH), pythonModulePath);
      // env.put(MyriaConstants.PYTHONPATH, pythonPath);

      // Set<String> keys = env.keySet();
      // for (String string : keys) {
      // String key = string;
      // String value = env.get(key);
      //
      // LOGGER.info("system var key :" + key);
      // LOGGER.info("value of sys var:" + value);
      // }

      pb.redirectError(Redirect.INHERIT);
      pb.redirectOutput(Redirect.INHERIT);
      // write the env variables to the path of the starting process
      Process codeReader = pb.start();

      OutputStream stdin = codeReader.getOutputStream();
      OutputStreamWriter out = new OutputStreamWriter(stdin, StandardCharsets.UTF_8);

      out.write(tmpSSocket.getLocalPort() + "\n");
      out.flush();
      Socket tmpClientSock = tmpSSocket.accept();
      setCode(tmpClientSock);

      // LOGGER.info("read code now closing sockets");

      if (tmpClientSock != null) {
        tmpClientSock.close();
      }

      if (tmpSSocket != null) {
        tmpSSocket.close();
      }
      if (codeReader != null) {
        codeReader.destroy();
      }

    } catch (IOException e) {
      e.printStackTrace();
    }

    return;
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
      // LOGGER.info("columns to write: " + columnIdx.length);
      try {
        // first write the number of columns being sent to the stream
        // dOut.writeInt(Integer.SIZE / Byte.SIZE);
        dOut.writeInt(columnIdx.length);
        // LOGGER.info("wrote number of columns to stream");

        for (int element : columnIdx) {
          // LOGGER.info("column number " + element);

          Type type = inputColumns.get(element).getType();
          // LOGGER.info("type: " + type.getName());
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
              LOGGER.info("about to write to stream, length of buffer " + input.array().length);
              dOut.writeInt(input.array().length);
              // LOGGER.info("length of stream " + dOut.size());
              LOGGER.info("length of buffer " + input.array().length);
              dOut.write(input.array());
              LOGGER.info("wrote buffer to stream");
              break;
          }
          dOut.flush();
          LOGGER.info("flushed output buffer ");

        }

      } catch (IOException e) {
        // LOGGER.info("IOException when writing to python process stream");
        throw new DbException(e);
      }

    }
  }

}
