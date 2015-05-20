/**
 *
 */
package edu.washington.escience.myria.operator.network;

import edu.washington.escience.myria.DbException;
import edu.washington.escience.myria.Schema;
import edu.washington.escience.myria.operator.CacheLeaf;
import edu.washington.escience.myria.operator.CacheRoot;
import edu.washington.escience.myria.parallel.CacheController;
import edu.washington.escience.myria.parallel.ExchangePairID;
import edu.washington.escience.myria.storage.TupleBatch;

/**
 * 
 */
public class CacheShuffleConsumer extends GenericShuffleConsumer {
  /**
   * @param schema input/output data schema
   * @param operatorID my operatorID
   * @param is from which workers the data will come.
   * */
  public CacheShuffleConsumer(final Schema schema, final ExchangePairID operatorID, final int[] is) {
    super(schema, operatorID, is);
    /* adding extra operators manually for now */
    // openCacheOperators();
  }

  /**
   * Starting to open the necessary cache operators (probably re-think this over).
   * */
  public void openCacheOperators() {
    // open a new root and make this it's child but it is forced to open CSC again
    CacheRoot cacheRootOperator = new CacheRoot(this);
    CacheLeaf cacheLeafOperator = new CacheLeaf();
    try {
      cacheRootOperator.open(getExecEnvVars());
      cacheLeafOperator.open(getExecEnvVars());
    } catch (DbException e) {
      e.printStackTrace();
    }
  }

  @Override
  protected final TupleBatch fetchNextReady() throws DbException {
    CacheController workerCacheController = getWorker().getCacheController();
    try {
      /* adding the tuple batch to the worker cache and returning for parent */
      TupleBatch receivedTb = getTuplesNormal(!nonBlockingExecution);
      workerCacheController.addTupleBatch(receivedTb);
      return receivedTb;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return null;
  }
}
