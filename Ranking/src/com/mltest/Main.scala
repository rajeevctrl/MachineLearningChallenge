package com.mltest

import scala.io.Source
import java.text.SimpleDateFormat

class Main {

  /**
   * This function fetches transaction data from {@param transactionFilePath}, parses that row 
   * and returns a map having data grouped by T1_T2_T4 level for each merchantID
   */
  def parseTransactionData(transactionFilePath: String): Map[String, Transaction] = {
    val merchantTransactions = scala.collection.mutable.Map[String, Transaction]();

    val linesIterator = Source.fromFile(transactionFilePath).getLines();
    val colNamesMap = this.getColNamesMap(linesIterator);

    //val merchantIDs=scala.collection.mutable.Set[String]();
    val linesIteratorHeaderRemoved = linesIterator.drop(1).filter { row => !row.contains("null") };
    linesIteratorHeaderRemoved.foreach { row =>
      {
        val tokens = row.split(",");
        val merchantID = tokens(colNamesMap.get("merchant_id").get);
        //merchantIDs.add(merchantID);
        val transaction = merchantTransactions.getOrElse(merchantID, Transaction(merchantID));
        val t4Key = s"${tokens(colNamesMap.get("T1").get)}_${tokens(colNamesMap.get("T2").get)}_${tokens(colNamesMap.get("T4").get)}";

        //Update total price
        val curRowPrice = tokens(colNamesMap.get("item_selling_price").get).toDouble * tokens(colNamesMap.get("qty_ordered").get).toDouble;
        val newTotalPrice = transaction.t4TotalPrice.getOrElse(t4Key, 0.0d) + curRowPrice
        val sla = {
          val shipByDate = Main.dateFormat.parse(tokens(colNamesMap.get("item_ship_by_date").get));
          val actualShipDate = Main.dateFormat.parse(tokens(colNamesMap.get("fulfillment_shipped_at").get));
          val breachStatus = if (actualShipDate.compareTo(shipByDate) > 0) true else false;
          val breachCount = if (breachStatus == true) transaction.t4TotalSLABreach.getOrElse(t4Key, 0.0d) + 1.0d else transaction.t4TotalSLABreach.getOrElse(t4Key, 1.0d);
          breachCount;
        }
        val totalOrders = transaction.t4TotalOrders.getOrElse(t4Key, 0.0d) + 1.0d;

        //Updte values in respective maps.
        transaction.t4TotalPrice(t4Key) = newTotalPrice;
        transaction.t4TotalSLABreach(t4Key) = sla;
        transaction.t4TotalOrders(t4Key) = totalOrders;

        //put in merchantTransactions
        merchantTransactions(merchantID) = transaction;

      }
    };
    //println(merchantTransactions.size)
    //println(merchantIDs.size)
    merchantTransactions.toMap;
  }

  
   /**
   * This function fetches profit data from {@param profitFilePath}, parses that row 
   * and returns a map having data grouped by T1_T2_T4 level for each merchantID
   */
  def parseProfitData(profitFilePath: String): Map[String, Profit] = {
    val linesIterator = Source.fromFile(profitFilePath).getLines();
    val colNamesMap = this.getColNamesMap(linesIterator);
    val linesIteratorHeaderRemoved = linesIterator.drop(1).filter { row => !row.contains("null") };

    //val merchantIDs=scala.collection.mutable.Set[String]();
    val profitData = scala.collection.mutable.Map[String, Profit]();
    linesIteratorHeaderRemoved.foreach { row =>
      {

        //Split each row and get respective merchantID and profit data.
        val tokens = row.split(",");
        val merchantID = tokens(colNamesMap.get("merchant_id").get);
        //merchantIDs.add(merchantID);
        val profit = profitData.getOrElse(merchantID, Profit(merchantID));
        val t4Key = s"${tokens(colNamesMap.get("T1").get)}_${tokens(colNamesMap.get("T2").get)}_${tokens(colNamesMap.get("T4").get)}";

        var commissionPercent = tokens(colNamesMap.get("commission_percent").get).toDouble
        var cashbackPercent = tokens(colNamesMap.get("cashback_percent").get).toDouble
        var discountPercent = tokens(colNamesMap.get("discount_percent").get).toDouble

        commissionPercent = profit.commissionPercent.getOrElse(t4Key, 0.0d) + commissionPercent;
        cashbackPercent = profit.cashbackPercent.getOrElse(t4Key, 0.0d) + cashbackPercent;
        discountPercent = profit.discountPercent.getOrElse(t4Key, 0.0d) + discountPercent;

        //Update values in map.
        profit.commissionPercent(t4Key) = commissionPercent;
        profit.cashbackPercent(t4Key) = cashbackPercent;
        profit.discountPercent(t4Key) = discountPercent;

        //put in profitData.
        profitData(merchantID) = profit;
      }
    };
    //println(merchantIDs.size)
    profitData.toMap;
  }

  
   /**
   * This function fetches return and cancel data from {@param returnCancelFilePath}, parses that row 
   * and returns a map having data grouped by T1_T2_T4 level for each merchantID
   */
  def parseReturnCancelData(returnCancelFilePath: String): Map[String, ReturnCancel] = {
    val linesIterator = Source.fromFile(returnCancelFilePath).getLines();
    val colNamesMap = this.getColNamesMap(linesIterator);
    val linesIteratorHeaderRemoved = linesIterator.drop(1).filter { row => !row.contains("null") };

    val merchantIDs = scala.collection.mutable.Set[String]();
    val returnCancelData = scala.collection.mutable.Map[String, ReturnCancel]();
    linesIteratorHeaderRemoved.foreach { row =>
      {
        val tokens = row.split(",");
        val merchantID = tokens(colNamesMap.get("merchant_id").get);
        merchantIDs.add(merchantID);
        val returnCancel = returnCancelData.getOrElse(merchantID, ReturnCancel(merchantID));
        val t4Key = s"${tokens(colNamesMap.get("T1").get)}_${tokens(colNamesMap.get("T2").get)}_${tokens(colNamesMap.get("T4").get)}";

        var cancelCount = tokens(colNamesMap.get("cancel_num").get).toDouble
        var returnCount = tokens(colNamesMap.get("return_num").get).toDouble

        cancelCount = returnCancel.cancelCount.getOrElse(t4Key, 0.0d) + cancelCount;
        returnCount = returnCancel.returnCount.getOrElse(t4Key, 0.0d) + returnCount;

        //Update values in map.
        returnCancel.cancelCount(t4Key) = cancelCount;
        returnCancel.returnCount(t4Key) = returnCount;

        returnCancelData(merchantID) = returnCancel;
      }
    };
    //println(merchantIDs.size);
    returnCancelData.toMap;
  }

  /**
   * This functions computes earnings of PayTM from respective merchants. For this firstly
   * we compute average price of each order by dividing total price of orders in a T4 level for a merchant
   * by total number of orders in that T4 level. Now as we know average price of an order, we could compute
   * the price of items canceled or returned. Thus net prices of products sold is total price minus price of returned
   * and cancelled orders. Now the actual earnings of PayTM are the commission it gets from actual sales minus 
   * the cash back it gives on those actual sales. Thus if a merchant has high amount of sales in a particular T4 level
   * then it will contribute more the earnings of PayTM.
   */
  def computT4LevelPayTMEarnings(merchantTransactionData: Map[String, Transaction],
    profitData: Map[String, Profit],
    returnCancelData: Map[String, ReturnCancel],
    merchantsWithAllData: Set[String]): Map[String, Map[String,Double]] = {

    val payTMEarnings = scala.collection.mutable.Map[String, Map[String,Double]]();

    val merchantT4LevelPayTMEarnings = merchantsWithAllData.toList.map { merchantID =>
      {
        val merchantProfitData: Profit = profitData.get(merchantID).get;
        val merchantReturnCancelData: ReturnCancel = returnCancelData.get(merchantID).get;
        val merchantTransData: Transaction = merchantTransactionData.get(merchantID).get;

        val t4KeySet = merchantProfitData.cashbackPercent.keySet.intersect(merchantReturnCancelData.cancelCount.keySet).intersect(merchantTransData.t4TotalOrders.keySet);
        val payTMEarningT4Level = t4KeySet.toList.map { t4Key =>
          {
            val cashBackPercent = merchantProfitData.cashbackPercent.get(t4Key).get;
            val commissionPercent = merchantProfitData.commissionPercent.get(t4Key).get;
            val discountPercent = merchantProfitData.discountPercent.get(t4Key).get;

            val cancelCount = merchantReturnCancelData.cancelCount.get(t4Key).get;
            val returnCount = merchantReturnCancelData.returnCount.get(t4Key).get;

            val totalOrders = merchantTransData.t4TotalOrders.get(t4Key).get;
            val totalPrice = merchantTransData.t4TotalPrice.get(t4Key).get;
            val avgPricePerOrder = totalPrice / totalOrders;

            val priceCancelOrders = cancelCount * avgPricePerOrder;
            val priceReturnOrders = returnCount * avgPricePerOrder;

            val actualSale = totalPrice - priceCancelOrders - priceReturnOrders;
            val actualEarningPayTM = (commissionPercent * actualSale) - (cashBackPercent * actualSale);

            (t4Key, actualEarningPayTM);
          }
        }.toMap;
      /*  val payTmEarning = PayTMEarnings(merchantID);
        payTmEarning.actualEarnings = payTMEarningT4Level;*/
        (merchantID, payTMEarningT4Level)
      }
    }.toMap;
    return merchantT4LevelPayTMEarnings;
  }
  
  /**
   * This function computes total earnings at each T4 level.
   */
  def totalEarningsPerT4Level(payTMEarningsAtT4Level: Map[String, Map[String,Double]]):Map[String,Double]={
    val t4LevelTotalEarnings=scala.collection.mutable.Map[String,Double]();
    
    payTMEarningsAtT4Level.foreach(row => {
      val merchantID=row._1;
      val payTMEarnings:Map[String,Double]=row._2
      payTMEarnings.foreach(x => {
        val newValue=t4LevelTotalEarnings.getOrElse(x._1, 0.0) + x._2;
        t4LevelTotalEarnings(x._1)=newValue;
      });
    })
    return t4LevelTotalEarnings.toMap;
  }
  
  /**
   * This function normalized earnings of paytm from each merchant at each T4 level.
   */
  def normalizePayTMEarnings(payTMEarningsAtT4Level: Map[String, Map[String,Double]] ,totalEarningsAtT4Level:Map[String,Double]):Map[String, Map[String, Double]]={
    val payTMNormalizedEarnings=payTMEarningsAtT4Level.map(row => {
      val merchantID=row._1;
      val earnings=row._2
      val normalizedEarnings=earnings.map(x => {
       (x._1,x._2/totalEarningsAtT4Level.get(x._1).get)
      }).toMap;
      (merchantID,normalizedEarnings);
    });
    return payTMNormalizedEarnings;
  }
  
  /**
   * This function computes score of each merchant at each T4 Level. Formula for score is :
   *  score=(discount  * Main.DISCOUNT_WEIGHT) + (normalizedPayTmEarnings * Main.PAYTM_EARNING_WEIGHT) + (Main.SLA_BREACH_WEIGHT/slaBreachProbability);
   */
  def computeT4LevelScore(normalizedEarnings:Map[String, Map[String, Double]],
      profitData: Map[String, Profit],
      merchantTransactionData: Map[String, Transaction],
      merchantsWithAllData:Set[String],
      t4Levels:Set[String]):Map[String, Double]={
    //Compute score of each merchant at each T4 level.
    val mearchantT4LevelScore=merchantsWithAllData.toList.map { merchantID => {
      val t4LevelScore=t4Levels.toList.map { t4Level => {
        val discount=profitData.get(merchantID).get.discountPercent.getOrElse(t4Level, 0.0d);
        val normalizedPayTmEarnings=normalizedEarnings.get(merchantID).get.getOrElse(t4Level, 0.0d);
        
        val totalSLABreaches=merchantTransactionData.get(merchantID).get.t4TotalSLABreach.getOrElse(t4Level, 0.0d);
        val totalRecords=merchantTransactionData.get(merchantID).get.t4TotalOrders.getOrElse(t4Level, 0.0d);
        
        val slaBreachProbability=totalSLABreaches/totalRecords;
        
        var score=(discount  * Main.DISCOUNT_WEIGHT) + (normalizedPayTmEarnings * Main.PAYTM_EARNING_WEIGHT) + (Main.SLA_BREACH_WEIGHT/slaBreachProbability);
        if(score.isNaN()) 
          score=0.0d;
        (t4Level,score);
      } };
      val sumScore=t4LevelScore.map(x => x._2).reduce((x,y) => x+y);
      val totalScore=sumScore/ t4Levels.size.toDouble;
      // val totalScore=sumScore;
      (merchantID,totalScore);
    } }.toMap;
    return mearchantT4LevelScore;
  }
  
  /**
   * This function computes average score of a merchant across all T4 levels and considers it overall score of the merchant.
   */
  def computeOverAllScores(t4LevelScores:Map[String, Map[String, Double]]):Map[String,Double]={
    val totalScores=t4LevelScores.map(row => {
      val merchantID= row._1;
      val t4LevelCount=row._2.size.toDouble;
      val scoreSum=row._2.map(x => x._2).reduce((x,y) => x+y)
      val avgScore=scoreSum/scoreSum;
      (merchantID,avgScore);
    });
    totalScores;
  }

  private def getColNamesMap(iterator: Iterator[String]): Map[String, Int] = {
    val colNamesMap = {
      var i = 0;
      val colNamesMap = iterator.take(1).next().split(",").map { colName =>
        {
          val value = (colName, i);
          i = i + 1;
          value;
        }
      }.toMap;
      colNamesMap;
    };
    return colNamesMap;
  }
}

object Main {
  val transactionFilePath: String = "/home/rajeev/Downloads/transaction_part-00000";
  val profiteFilePath: String = "/home/rajeev/Downloads/profitMetrics_part-00000";
  val returnCancelFilePath: String = "/home/rajeev/Downloads/returnedCancelledMetrics_part-00000";
  val DISCOUNT_WEIGHT=0.4;
  val PAYTM_EARNING_WEIGHT=0.4;
  val SLA_BREACH_WEIGHT=0.2;
  
  val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  

  def main(args: Array[String]): Unit = {
    val main = new Main();
    
    println("Parsing transaction data");
    val merchantTransactionData: Map[String, Transaction] = main.parseTransactionData(transactionFilePath);
    //println(merchantTransactionData.size);

    println("Parsing profit data");
    val profitData: Map[String, Profit] = main.parseProfitData(profiteFilePath);
    //println(profitData.size);

    println("Parsing return and cancel data");
    val returnCancelData: Map[String, ReturnCancel] = main.parseReturnCancelData(returnCancelFilePath);
    //println(returnCancelData.size);

    val merchantsWithAllData = merchantTransactionData.keySet.intersect(profitData.keySet).intersect(returnCancelData.keySet);
    println(s"Total ${merchantsWithAllData.size} merchants have data in all three files. Processing these merchants for Ranking");

    val payTMEarningsAtT4Level: Map[String, Map[String,Double]] = main.computT4LevelPayTMEarnings(merchantTransactionData, profitData, returnCancelData, merchantsWithAllData);
    println(payTMEarningsAtT4Level.size);
    //payTMEarningsAtT4Level.take(10).foreach(x => println(x._1+"           "+x._2))
    
    println("Computing total earnings by PayTM at each T4 level.");
    val totalEarningsAtT4Level:Map[String,Double]=main.totalEarningsPerT4Level(payTMEarningsAtT4Level);
    totalEarningsAtT4Level.take(10).foreach(x => println(x._1+"        "+x._2))
    
    println("Normalizing earnings");
    val normalizedEarnings:Map[String, Map[String, Double]]=main.normalizePayTMEarnings(payTMEarningsAtT4Level, totalEarningsAtT4Level);
    normalizedEarnings.take(10).foreach(x => println(x._1+"        "+x._2))
    
   // val t4LevelScores:Map[String, Map[String, Double]]=main.computeT4LevelScore(normalizedEarnings, profitData, merchantTransactionData, merchantsWithAllData, totalEarningsAtT4Level.keySet);
   // val scores:Map[String,Double]=main.computeOverAllScores(t4LevelScores).toSeq.sortWith((x,y) => x._2 > y._2).toMap;
    println("Computing scores")
    val scores:Map[String, Double]=main.computeT4LevelScore(normalizedEarnings, profitData, merchantTransactionData, merchantsWithAllData, totalEarningsAtT4Level.keySet);
   
    val nanScores=scores.filter(x => x._2.isNaN())
    println("Nan Scores : "+nanScores.size)
    
    println("Sorting scores");
    val sortedScores=scores.toSeq.sortWith((x,y) => x._2 > y._2);
    sortedScores.take(10).foreach(row => println(s"MerchantID : ${row._1}     Score : ${row._2}"))
    
  }

}

case class Transaction(val merchantID: String) {
  val t4TotalPrice: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]();
  val t4TotalSLABreach: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]();
  val t4TotalOrders: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]();
}

case class Profit(val merchantID: String) {
  val commissionPercent: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]();
  val cashbackPercent: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]();
  val discountPercent: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]();
}

case class ReturnCancel(val merchantID: String) {
  val cancelCount: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]();
  val returnCount: scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]();
}

