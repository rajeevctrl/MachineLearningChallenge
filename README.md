# Machine Learning Challenge (Solution)

Computation of scores of mearchants dependes on following metric.
  1. Discount given by merchant.
  2. Amount earned by PayTM from a merchant.
  3. SLA breach rate of shipping time.


Proportionality of Score with each of above points.
  1. Score directly proportional to Discount : Discount is a positive metric w.r.t customer. More is the discount given by a merchant, better is the merchant as extent of discount helps in increasing the amount of sale, thus increasing overall profit.
  2. Score directly proportional to Amount earned by PayTM : Amount earned by PayTM is a positive metric w.r.t PayTM. More the amount PayTM earns from a merchant, better is the Score of that merchant.
  3. SLA breach is inversly proportional to Score : SLA breach is a negative metric w.r.t both consumer and PayTM. SLA breach directly relates to late delivery and carelessness of merchant, leading to probable cancellation of order.

## Computation of above metrics
1. Discount : discount is considered as given in [Profit Metrics] file.
2. Amount earned by PayTM : Computation of this metric is as follows.

 a. Compute total price of orders for a particular T4 level for a particular merchant.
 
 b. Compute total number of orders for a particular T4 level for a particular merchant.
 
 c. Compute average cost for each T4 level for each merchant.
 
      Average cost of each order for T4 level = total price / total orders.
 
 d. Now returns and cancellation of orders reduce overall money gained from sales. So compute amount lost due to return and cancellation for a particular T4 for a particular merchant:
 
      Amount loss due to return= num orders returned * average price of an order
 
      Amount loss due to cancellation= num orders cancelled * average price of an order.
 
 g. Now actual amount earned is: 
 
      Actual Amount earned = Total price from transactions - Amount loss due to Return - Amount loss due to Cancellation
      
 h. Now we need to normalize this amount w.r.t sum of actual amount earned by all merchants for a single T4.
 
      Normalized actual amount earned = amount earned by single merchant / sum(amount earned by all merchants)

3. SLA breach : Computation of this metric is as follows:

  a. Fetch 'item_ship_by_date' and 'fulfillment_shipped_at' values of the row in transaction table.
  
  b. Convert these values to Date type of java.util.Date
  
  c. Compare these values if 'item_ship_by_date' is before 'fulfillment_shipped_at' then SLA is breached else not.
  
  d. Compute sum of SLA breaches for a particular T4 for a merchant.
  
  e. Find Probability(SLA Breach):
  
      Probability(SLA Breach) = count SLA Breaches / Total rows for a particular T4 for a particular customer.

## Computation of Score for a particular T4 level
Now that we have computed above metrics at T4 level for each merchant. Next we will be computing T4 Level scores for each merchant.

1. Take following 3 parameters as input. These specify the user centric weights to be given to different metrics while computing scores:

  a. DISCOUNT_WEIGHT : a real number in range [0,1] specifying the weight to be given to the amount of discount offered by merchant.
  
  b. PAYTM_EARNING_WEIGHT : a real number in range [0,1] specifying the weight to be given to the earnings by PayTM while computing score.
  
  c. SLA_BREACH_WEIGHT : a real number in range [0,1] specifying the weight to be given to the probability of SLA breack while computing scores.

Once user configures all these weights the score could be computed by formula:

    T4 Level score = (discount  * DISCOUNT_WEIGHT) + (normalizedPayTmEarnings * PAYTM_EARNING_WEIGHT) + (LA_BREACH_WEIGHT/slaBreachProbability)

## Computing a single score for each merchant
This score gives a single score value to each merchant which is an aggregated values all positive and negative metrics:

    Score merchant = Sum(T4 Level score of merchant for each T4 level) / total number of T4 levels

## Steps to run the program
1. Provide values of following parameters included in Main object of SCALA program
  
  a. val transactionFilePath: String = "/home/rajeev/Downloads/transaction_part-00000";
  
  b. val profiteFilePath: String = "/home/rajeev/Downloads/profitMetrics_part-00000";
  
  c. val returnCancelFilePath: String = "/home/rajeev/Downloads/returnedCancelledMetrics_part-00000";
  
  d. val DISCOUNT_WEIGHT=0.4;
  
  e. val PAYTM_EARNING_WEIGHT=0.4;
  
  f. val SLA_BREACH_WEIGHT=0.2;

2. Execute main() method of Main scala object.




