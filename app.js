const express = require('express');
const { Client } = require('@elastic/elasticsearch');
const bodyParser = require('body-parser');
const path = require('path');
const { log } = require('console');

const app = express();
const es = new Client({ node: 'http://10.150.238.177:9200'  });

app.use(bodyParser.json());

let rawData = [];

app.get('/data', async (req, res) => {
    rawData = [];
    try {
        const response = await es.search({
            index: 'jaeger-span-2024-07-17',
            body: {
                query: {
                    bool: {
                        must: [
                            { match: { 'operationName': 'POST /api/transaction' } },
                            { match: { 'process.serviceName': 'unknown_service:node' } },
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
                if (tag.key === 'http.response.post') {
                    const responseBody = JSON.parse(tag.value);
                    rawData.push(responseBody);
                    const sourceAccount = responseBody.senderAccount;
                    const destinationAccount = responseBody.receiverAccount;

                    // Source account data (money spent)
                    data.push({
                        userId: sourceAccount.User.user_id,
                        userName: sourceAccount.User.name,
                        accountNumber: sourceAccount.account_id,
                        bankName: sourceAccount.Bank.name,
                        total_spent: responseBody.amount,
                        total_received: 0,
                        transaction_count: 1
                    });

                    // Destination account data (money received)
                    data.push({
                        userId: destinationAccount.User.user_id,
                        userName: destinationAccount.User.name,
                        accountNumber: destinationAccount.account_id,
                        bankName: destinationAccount.Bank.name,
                        total_spent: 0,
                        total_received: responseBody.amount,
                        transaction_count: 1
                    });
                }
            });
    });

    if (data.length > 0) {

        const summary = data.reduce((acc, curr) => {
            const key = `${curr.userId}-${curr.userName}-${curr.accountNumber}-${curr.bankName}`;
            if (!acc[key]) {
                acc[key] = { ...curr };
            } else {
                acc[key].total_spent += curr.total_spent;
                acc[key].total_received += curr.total_received;
                acc[key].transaction_count += curr.transaction_count;
            }
            return acc;
        }, {});

        const summaryArray = Object.values(summary);

        res.json(summaryArray);
    } else {
        res.json([]);
    }
} catch (error) {
    console.error(error);
    res.status(500).json({ error: 'An error occurred while fetching data' });
}

});

app.get('/data/user/:id', (req, res) => {

    const userId = parseInt(req.params.id);
    let userData = [];

    rawData.forEach(data => {
        if(data.senderAccount.User.user_id === userId){
            userData.push({
                senderID: data.senderAccount.User.user_id,
                senderName: data.senderAccount.User.name,
                receiverID: data.receiverAccount.User.user_id,
                receiverName: data.receiverAccount.User.name,
                total_spent:data.amount,
                total_received:0,
                transaction_count:1
            });

        }

        if(data.receiverAccount.User.user_id === userId){

            userData.push({
                senderID: data.senderAccount.User.user_id,
                senderName: data.senderAccount.User.name,
                receiverID: data.receiverAccount.User.user_id,
                receiverName: data.receiverAccount.User.name,
                total_spent:0,
                total_received:data.amount,
                transaction_count:1
            });

        }




    })

    if (userData.length > 0) {
        const summary = userData.reduce((acc, curr) => {
            const key = `${curr.senderID}-${curr.receiverID}`;
            if (!acc[key]) {
                acc[key] = { ...curr };
            } else {
                acc[key].total_spent += curr.total_spent;
                acc[key].total_received += curr.total_received;
                acc[key].transaction_count += curr.transaction_count;
            }
            return acc;
        }, {});

        const summaryArray = Object.values(summary);

        res.json(summaryArray);
    } else {
        res.json([]);
    }

});

app.get('/data/bank/:id', (req, res) => {

    const bankId = parseInt(req.params.id);
    let bankData = [];


    rawData.forEach(data => {
        if(data.senderAccount.Bank.bank_id === bankId){
            bankData.push({
                senderBankID:data.senderAccount.Bank.bank_id,
                senderBankName:data.senderAccount.Bank.name,
                senderID: data.senderAccount.User.user_id,
                senderName: data.senderAccount.User.name,
                receiverBankID:data.receiverAccount.Bank.bank_id,
                receiverBankName: data.receiverAccount.Bank.name,
                receiverID: data.receiverAccount.User.user_id,
                receiverName: data.receiverAccount.User.name,
                total_transaction:data.amount,
                transaction_count:1
            });
        }

        //receive
        if(data.receiverAccount.Bank.bank_id === bankId){
            bankData.push({
                senderBankID:data.senderAccount.Bank.bank_id,
                senderBankName:data.senderAccount.Bank.name,
                senderID: data.senderAccount.User.user_id,
                senderName: data.senderAccount.User.name,
                receiverBankID:data.receiverAccount.Bank.bank_id,
                receiverBankName: data.receiverAccount.Bank.name,
                receiverID: data.receiverAccount.User.user_id,
                receiverName: data.receiverAccount.User.name,
                total_transaction:data.amount,
                transaction_count:1
            });
        }
        
    })

    if (bankData.length > 0) {
        const summary = bankData.reduce((acc, curr) => {
            const key = `${curr.senderID}-${curr.receiverID}`;
            if (!acc[key]) {
                acc[key] = { ...curr };
            } else {
                acc[key].total_transaction += curr.total_transaction;
                acc[key].transaction_count += curr.transaction_count;
            }
            return acc;
        }, {});

        const summaryArray = Object.values(summary);

        res.json(summaryArray);
    } else {
        res.json([]);
    }
});

/*app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'views', 'index.html'));
});*/

const PORT = 5008;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}.`);
  });
