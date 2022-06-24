//https://www.mongodb.com/docs/atlas/app-services/triggers/scheduled-triggers/

exports = function() {
    // Instantiate MongoDB collection handles
    const mongodb = context.services.get("mongodb-atlas");
    const orders = mongodb.db("store").collection("orders");
    const reports = mongodb.db("store").collection("reports");
  
    // Generate the daily report
    return orders.aggregate([
      // Only report on orders placed since yesterday morning
      { $match: {
          orderDate: {
            $gte: makeYesterdayMorningDate(),
            $lt: makeThisMorningDate()
          }
      } },
      // Add a boolean field that indicates if the order has already shipped
      { $addFields: {
          orderHasShipped: {
            $cond: {
              if: "$shipDate", // if shipDate field exists
              then: 1,
              else: 0
            }
          }
      } },
      // Unwind individual items within each order
      { $unwind: {
          path: "$orderContents"
      } },
      // Calculate summary metrics for yesterday's orders
      { $group: {
          _id: "$orderDate",
          orderIds: { $addToSet: "$_id" },
          numSKUsOrdered: { $sum: 1 },
          numItemsOrdered: { $sum: "$orderContents.qty" },
          totalSales: { $sum: "$orderContents.price" },
          averageOrderSales: { $avg: "$orderContents.price" },
          numItemsShipped: { $sum: "$orderHasShipped" },
      } },
      // Add the total number of orders placed
      { $addFields: {
          numOrders: { $size: "$orderIds" }
      } }
    ]).next()
      .then(dailyReport => {
        reports.insertOne(dailyReport);
      })
      .catch(err => console.error("Failed to generate report:", err));
  };
  
  function makeThisMorningDate() {
    return setTimeToMorning(new Date());
  }
  
  function makeYesterdayMorningDate() {
    const thisMorning = makeThisMorningDate();
    const yesterdayMorning = new Date(thisMorning);
    yesterdayMorning.setDate(thisMorning.getDate() - 1);
    return yesterdayMorning;
  }
  
  function setTimeToMorning(date) {
    date.setHours(7);
    date.setMinutes(0);
    date.setSeconds(0);
    date.setMilliseconds(0);
    return date;
  }