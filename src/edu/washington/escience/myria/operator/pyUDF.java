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
  private final String pythonExec = "/home/ubuntu/anaconda2/bin/python2.7";
  private ServerSocket serverSocket = null;
  private Socket clientSoc = null;
  private Process worker = null;

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
  public pyUDF(final String filename, final Operator child) {
    super(child);
    if (null != filename) {
      this.filename = filename;
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

    List<? extends Column<?>> inputColumns = tb.getDataColumns();

    for (int i = 0; i < rows; i++) {
      ByteBuffer input = null;
      // look up the schema to figure out which column is BYTE_TYPE and read it in.

      for (int j = 0; j < tb.numColumns(); j++) {
        if (inputColumns.get(j).getType() == Type.BYTES_TYPE) {
          input = inputColumns.get(j).getByteBuffer(i);
        }
      }
      LOGGER.info("trying to launch worker for row: " + i);

      if (input != null) {
        // instead of launch a worker per tuple, and then kill it,
        // run a process per worker and reuse it.
        Startworker();
        if (clientSoc != null) {

          LOGGER.info("successfully launched worker");

          DataOutputStream dOut = new DataOutputStream(clientSoc.getOutputStream());

          dOut.writeInt(input.array().length);
          dOut.write(input.array());
          dOut.flush();

          LOGGER.info("wrote to  output stream for the client socket");
          // get results back!

          DataInputStream dIn = new DataInputStream(clientSoc.getInputStream());

          int length = 0;
          try {
            length = dIn.readInt(); // read length of incoming message
          } catch (Exception e) {
            length = 0;
            LOGGER.info("Error reading int from stream");
          }
          if (length >= 0) {
            byte[] b = new byte[length];
            dIn.read(b);
            output.putByteBuffer(0, ByteBuffer.wrap(b));
          } else {
            byte[] b = "empty".getBytes();

            output.putByteBuffer(0, ByteBuffer.wrap(b));
            LOGGER.info("worker wrote int length less than zero ");
          }

          // close client socket
          clientSoc.close();

        } else {
          byte[] b = "empty".getBytes();

          output.putByteBuffer(0, ByteBuffer.wrap(b));
          LOGGER.info("Failed launching worker");
        }
        if (serverSocket != null) {
          serverSocket.close();
        }
      }

    }

    return output.popAnyUsingTimeout();

  }

  private void Startworker() throws UnknownHostException, IOException {

    serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
    int a = serverSocket.getLocalPort();
    LOGGER.info("created socket " + a);

    ProcessBuilder pb = new ProcessBuilder(pythonExec, filename);

    pb.redirectError(Redirect.INHERIT);
    pb.redirectOutput(Redirect.INHERIT);
    worker = pb.start();

    OutputStream stdin = worker.getOutputStream();
    OutputStreamWriter out = new OutputStreamWriter(stdin, StandardCharsets.UTF_8);

    out.write(serverSocket.getLocalPort() + "\n");
    out.flush();
    clientSoc = serverSocket.accept();

    return;

  }

  @Override
  protected void init(final ImmutableMap<String, Object> execEnvVars) throws DbException, IOException {

    Preconditions.checkNotNull(filename);
    // get the worker socket to read and write.

  }

  @Override
  public Schema generateSchema() {
    return SCHEMA;
  }

}
