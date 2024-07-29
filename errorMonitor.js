const express = require('express');
const { Client, estypesWithBody } = require('@elastic/elasticsearch');
const bodyParser = require('body-parser');
const path = require('path');

const app = express();
const es = new Client({ node: 'http://10.150.238.177:9200'  });

app.use(bodyParser.json());

let rawData = [];

app.get('/data', async (req, res) => {
    rawData = [];
    try {
        const response = await es.search({
            index: 'jaeger-span-2024-07-23',
            body: {
                query: {
                    bool: {
                        must: [

                            { match: { 'process.serviceName': 'nodejs_bank_api' } },
                            {
                                nested: {
                                    path: 'tags',
                                    query: {
                                        bool: {
                                            must: [
                                                { match: { 'tags.key': 'http.response.error' } }
                                            ]
                                        }
                                    }
                                }
                            }
                        ]
                    }
                },
                _source: ['operationName', 'tags'],
                from: 0,
                size: 1000
            }
        });

        let data = [];
        response.hits.hits.forEach(hit => {
            const tags = hit._source.tags;
            tags.forEach(tag => {

                if (tag.key === 'http.response.error') {
                    const responseBody = JSON.parse(tag.value);
                    rawData.push(responseBody);


                    // Source account data (money spent)
                    data.push({
                        errorNo: responseBody.error_no,
                        errorDescription: responseBody.error_description,
                        errorInfo: responseBody.error_info
                    });
                }
            });
    });

    if (data.length > 0) {
        res.json(data);
    } else {
        res.json([]);
    }
} catch (error) {
    console.error(error);
    res.status(500).json({ error: 'An error occurred while fetching data' });
}

});


app.get('/data/bankerror/:id', (req, res) => {

    const bankId = parseInt(req.params.id);
    let bankData = [];


    rawData.forEach(data => {
        if(Object.entries(data.error_info).length !== 0){
            if(data.error_info.account.Bank.bank_id === bankId){
                bankData.push({
                    bankName: data.error_info.account.Bank.name,
                    errorNo: data.error_no,
                    errorDescription: data.error_description
                });
            }
        }    
    })

    if (bankData.length > 0) {

        res.json(bankData);
    } else {
        res.json([]);
    }
});


//iki farklı yolu var

app.get('/data/timeinterval/:id',async (req,res) => {

    const bankId = parseInt(req.params.id);
    let bankData = [];
    

    try {
        const response = await es.search({
            index: 'jaeger-span-2024-07-24', // Your index name
            body: {
                query: {
                    bool: {
                      must: [
                        { match: { 'process.serviceName': 'nodejs_bank_api' } },
                        {
                          nested: {
                            path: 'tags',
                            query: {
                              bool: {
                                must: [
                                  { match: { 'tags.key': 'http.response.post' } }
                                ]
                              }
                            }
                          }
                        },
                        /*{
                          range: {
                            startTimeMillis: {
                              gte: '2024-07-01T07:30:00.000',
                              lte: '2024-07-31T07:50:00.000',
                              format: "yyyy-MM-dd'T'HH:mm:ss.SSS"
                            }
                          }
                        }*/
                      ]
                    }
                  },
                  _source: ['operationName', 'tags'],
                  from: 0,
                  size: 1000
            }
          });
        const startTime = new Date('Wed Jul 24 2024 07:30:00 GMT+0300 (GMT+03:00)'); //7.30
        const endTime = new Date('Wed Jul 24 2024 07:50:00 GMT+0300 (GMT+03:00)'); // 7.44

        response.hits.hits.forEach(hit => {
            const tags = hit._source.tags;
            tags.forEach(tag => {


                if (tag.key === 'http.response.post') {
                    const responseBody = JSON.parse(tag.value);
                    
                    const sourceAccount = responseBody.senderAccount;
                    const destinationAccount = responseBody.receiverAccount;
                    const transactionTime = new Date(responseBody.transaction_time);

                    if (sourceAccount.Bank.bank_id === bankId || destinationAccount.Bank.bank_id === bankId) {

                        if (transactionTime > startTime && transactionTime < endTime) {
                            
                            bankData.push({
                                senderBankID: sourceAccount.Bank.bank_id,
                                senderBankName: sourceAccount.Bank.name,
                                senderID: sourceAccount.User.user_id,
                                senderName: sourceAccount.User.name,
                                receiverBankID: destinationAccount.Bank.bank_id,
                                receiverBankName: destinationAccount.Bank.name,
                                receiverID: destinationAccount.User.user_id,
                                receiverName: destinationAccount.User.name,
                                amount: responseBody.amount
                            });
                        }
                    }
                }


            });
    });

    if (bankData.length > 0) {
        res.json(bankData);
    } else {
        res.json([]);
    }
} catch (error) {
    console.error(error);
    res.status(500).json({ error: 'An error occurred while fetching data' });
}

});




app.get('/durations', async (req, res) => {

  try {
      const response = await es.search({
          index: 'jaeger-span-2024-07-24', // Your index name
          body: {
            query: {
              bool: {
                must: [
                  { match: { 'process.serviceName': 'nodejs_bank_api' } }
                ],
                filter: [
                  {
                    terms: {
                      "operationName": [
                        "POST /api/transaction",
                        "PUT /api/transaction/withdraw/:id",
                        "PUT /api/transaction/deposit/:id",
                      ]
                    }
                  }
                ]
              }
            },
            _source: ['operationName', 'tags','duration'],
            from: 0,
            size: 1000,
          }
        });

        let durationData = []
        response.hits.hits.map(hit => {
          durationData.push({
             operationName : hit._source.operationName || 'Unknown',
             duration : hit._source.duration || 0
          });
      });

      res.send(durationData);

} catch (error) {
  console.error(error);
  res.status(500).send('An error occurred');
}

});







app.get('/percentiles', async (req, res) => {

    try {
        const response = await es.search({
            index: 'jaeger-span-2024-07-24', // Your index name
            body: {
              query: {
                bool: {
                  must: [
                    { match: { 'process.serviceName': 'nodejs_bank_api' } }
                  ],
                  filter: [
                    {
                      terms: {
                        "operationName": [
                          "POST /api/transaction",
                          "PUT /api/transaction/withdraw/:id",
                          "PUT /api/transaction/deposit/:id",
                        ]
                      }
                    }
                  ]
                }
              },
              _source: ['operationName', 'tags'],
              from: 0,
              size: 1000,
              aggs: {
                by_operation: {
                  terms: {
                    field: "operationName",
                    size: 10
                  },
                  aggs: {
                    load_time_percentiles: {
                      percentiles: {
                        field: "duration",
                        percents: [50, 75, 90, 95, 99]
                      }
                    }
                  }
                }
              }
            }
          });

   // Process the response
    const percentilesData = {};
    response.aggregations.by_operation.buckets.forEach(bucket => {
        const operationName = bucket.key;
        const percentiles = bucket.load_time_percentiles.values;
        percentilesData[operationName] = {};
        for (const [k, v] of Object.entries(percentiles)) {
            percentilesData[operationName][k] = v / 1000; // Convert to milliseconds
        }
    });

    // Send the response as JSON
    res.json(percentilesData);
} catch (error) {
    console.error(error);
    res.status(500).send('An error occurred');
}

});

app.get('/slowest', async (req, res) => {
  try {
      const response = await es.search({
          index: 'jaeger-span-2024-07-24',
          body: {
              query: {
                  bool: {
                      must: [
                          { match: { 'process.serviceName': 'nodejs_bank_api' } }
                      ]
                  }
              },
              _source: ['operationName', 'duration'],
              from: 0,
              size: 1000,
              sort: [
                  { duration: { order: 'desc' } }
              ]
          }
      });

  const slowestOperations = response.hits.hits.slice(0, 5).map(hit => {
      const operationName = hit._source.operationName || 'Unknown';
      const duration = hit._source.duration || 0;
      return {
          operationName,
          duration_ms: duration / 1000
      };
  });

  res.json(slowestOperations);
} catch (error) {
  console.error(error);
  res.status(500).send('An error occurred');
}

});





/*app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'views', 'index.html'));
});*/




const PORT = 5008;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}.`);
  });
