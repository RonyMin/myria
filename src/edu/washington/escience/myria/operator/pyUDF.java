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
public class pyUDF extends UnaryOperator {
  /***/
  private static final long serialVersionUID = 1L;

  private String filename;
  // // this is temporarily the pickled object that is sent back
  public static final Schema SCHEMA = Schema.ofFields(Type.BYTES_TYPE, "UDF");

  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(pyUDF.class);
  // private final String pythonExec = "/home/ubuntu/anaconda2/bin/python2.7";
  private final String pythonExec = "/Users/parmita/anaconda/bin/python";
  // private final String pythonExec = "python";
  private ServerSocket serverSocket = null;
  private Socket clientSoc = null;
  private Process worker = null;
  private int[] columnIdx = null;

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
  public pyUDF(final String filename, final int[] columnIdx, final Operator child) {
    super(child);
    if (null != filename) {
      this.filename = filename;
      this.columnIdx = columnIdx;
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
    final TupleBatchBuffer output = new TupleBatchBuffer(SCHEMA);

    Startworker();
    if (clientSoc != null) {
      LOGGER.info("successfully launched worker");
      try {
        DataOutputStream dOut = new DataOutputStream(clientSoc.getOutputStream());
        DataInputStream dIn = new DataInputStream(clientSoc.getInputStream());

        for (int i = 0; i < rows; i++) {
          // write to stream
          if (writeToStream(dOut, tb, i)) {
            LOGGER.info("Wrote to stream successfully!");
            if (readFromStream(dIn, tb, i, output)) {
              LOGGER.info("successfully read from the stream");
            }

          } else {
            LOGGER.info("Error writing to python stream");
            sendErrorbuffer(output);
          }
        }
      } catch (IOException e) {
        LOGGER.info("Exception getting stream from python process");

      }

    } else {
      LOGGER.info("could not launch worker");
      sendErrorbuffer(output);
    }
    // close client socket
    if (clientSoc != null) {
      clientSoc.close();
    }

    if (serverSocket != null) {
      serverSocket.close();
    }

    return output.popAnyUsingTimeout();

  }

  private boolean readFromStream(final DataInputStream dIn, final TupleBatch tb, final int row,
      final TupleBatchBuffer output) {
    int length = 0;
    try {
      length = dIn.readInt(); // read length of incoming message
      if (length > 0) {
        byte[] b = new byte[length];
        dIn.read(b);
        output.putByteBuffer(0, ByteBuffer.wrap(b));
        LOGGER.info("read bytes from python process " + length);
        return true;
      }
    } catch (Exception e) {
      length = 0;
      LOGGER.info("Error reading int from stream");
    }
    return false;
  }

  private boolean writeToStream(final DataOutputStream dOut, final TupleBatch tb, final int row) {
    List<? extends Column<?>> inputColumns = tb.getDataColumns();
    for (int element : columnIdx) {
      // LOGGER.info("column number " + element);

      Type type = inputColumns.get(element).getType();
      // LOGGER.info("type: " + type.getName());

      try {
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
            dOut.writeInt(input.array().length);
            LOGGER.info("length of buffer " + input.array().length);
            dOut.write(input.array());
            break;

        }
        dOut.flush();
      } catch (IOException e) {
        // LOGGER.info("IOException when writing to python process stream");
        e.printStackTrace();
        return false;

      }

    }

    return true;

  }

  private void sendErrorbuffer(final TupleBatchBuffer output) {
    byte[] b = "empty".getBytes();

    output.putByteBuffer(0, ByteBuffer.wrap(b));
    // LOGGER.info("Failed launching worker");
  }

  private void Startworker() throws UnknownHostException, IOException {

    serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
    int a = serverSocket.getLocalPort();
    LOGGER.info("created socket " + a);

    ProcessBuilder pb = new ProcessBuilder(pythonExec, filename);

    pb.redirectError(Redirect.INHERIT);
    pb.redirectOutput(Redirect.INHERIT);
    // write the env variables to the path of the starting process
    worker = pb.start();

    OutputStream stdin = worker.getOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(stdin, StandardCharsets.UTF_8);

    out.write(serverSocket.getLocalPort() + "\n");
    out.flush();
    clientSoc = serverSocket.accept();

    return;

  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException {

    Preconditions.checkNotNull(filename);
    // send out the python script to each worker.
    if (execEnvVars != null && !execEnvVars.containsKey(MyriaConstants.PYTHON_RUNNER)) {
      // create a pythonrunner and add it to the execEnvVars
      LOGGER.info("need to create python process ");
    } else {
      // get the pythonrunner and use it from here
      LOGGER.info("python process exisits - using it now");
      // / private Socket clientSoc = null;

    }

  }

  @Override
  public Schema generateSchema() {
    return SCHEMA;
  }

}
