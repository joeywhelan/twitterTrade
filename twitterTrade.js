/**
 * @fileoverview Functions implementing stock trade alerts on a Twitter feed
 * @author Joey Whelan <joey.whelan@gmail.com>
 */

'use strict';
'use esversion 6';
const fetch = require('node-fetch');
const btoa = require('btoa');
const AbortController = require('abort-controller');
const fs = require('fs');
const fsp = fs.promises;
const winston = require('winston');
require('winston-daily-rotate-file');

const ABORT_TIMEOUT = 90; //time, in seconds, to abort a streaming connection

const TWITTER_KEY = process.env.TWITTER_KEY;  //twitter auth key
const TWITTER_SECRET = process.env.TWITTER_SECRET;  //twitter auth secret
const AUTH_URL   = 'https://api.twitter.com/oauth2/token';  //url for fetching a twitter bearer token
const RULES_URL  = 'https://api.twitter.com/labs/1/tweets/stream/filter/rules';
const STREAM_URL = 'https://api.twitter.com/labs/1/tweets/stream/filter?format=compact';
const RULES = [{'value' : 'from:realDonaldTrump -is:retweet -is:quote'}];

const IEX_KEY = process.env.IEX_KEY;  //IEX API key
const SYMBOL_URL = 'https://cloud.iexapis.com/stable/ref-data/symbols?token=';
const STOCK_URL = 'https://cloud.iexapis.com/stable/stock/';

const GOOGLE_KEY = process.env.GOOGLE_KEY;  //Google NL API key
//Google NL Entity/Sentiment REST end point
const ENTITY_SENTIMENT_URL = 'https://language.googleapis.com/v1beta2/documents:analyzeEntitySentiment?key=';
//Google NL Sentiment REST end point
const SENTIMENT_URL = 'https://language.googleapis.com/v1beta2/documents:analyzeSentiment?key=';

const SENDGRID_KEY = process.env.SENDGRID_KEY;
const SENDGRID_URL = 'https://api.sendgrid.com/v3/mail/send';
const RECIPIENT_FILE = process.env.RECIPIENT_FILE;

let g_reader;
let g_backoff = 0;
const logger = winston.createLogger({
    transports: [
        new winston.transports.Console(),
        new (winston.transports.DailyRotateFile)({
            filename: 'twitter-%DATE%.log',
            datePattern: 'YYYY-MM-DD-HH',
            zippedArchive: true,
            maxSize: '20m',
            maxFiles: '10d'
          })
    ],
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.align(),
      winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
    ),
    level: 'debug'
});
let symbolArray;

/**
 * Function that clears out existing rules on a twitter realtime filter
 * @param {string} token - twitter bearer token
 * @param {string} url - url to the twitter filter rules API
 * @return {promise} integer representing number of rules deleted
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function clear(token, url) {
    logger.debug(`clearAllRules()`);

    const rules = await getRules(token, url);  //fetch all the existing rules
    if (Array.isArray(rules.data)) {
        const ids = rules.data.map(rule => rule.id);
        const deleted = await deleteRules(token, url, ids);  //delete them
        return deleted;
    }
    else {
        return 0;
    }
}

/**
 * Function that deletes an array of ids on a twitter realtime filter
 * @param {string} token - twitter bearer token
 * @param {string} url - url to the twitter filter rules API
 * @param {Array} ids - array rule ids to delete
 * @return {promise} integer representing number of rules deleted
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function deleteRules(token, url, ids) {
    logger.debug(`deleteRules()`);
 
    const body = {
        'delete' : {
            'ids': ids
        }
    };
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type' : 'application/json',
                'Authorization' : 'Bearer ' + token
            },
            body: JSON.stringify(body)
        });
        if (response.ok) {
            const json = await response.json();
            return json.meta.summary.deleted;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        logger.error(`deleteRules() - ${err}`);
        throw err;
    }
}

/**
 * Function that calls Google Natural Language entitySentiment REST end point to derive sentiment
 * on the top salience entity within a tweet
 * @param {string} key - Google API key
 * @param {string} url - Goggle NL Rest endpoint for entitySentiment
 * @param {string} tweet - text of a tweet
 * @return {promise} JSON object containing entity-sentiment analysis of the top salience entity
 * @throws {Error} propagates HTTP status errors or node fetch exceptions
 */
async function entitySentiment(key, url, tweet) {
    logger.debug(`entitySentiment()`);

    const document = {
        'type': 'PLAIN_TEXT',
        'language': 'en',
        'content': tweet
    };

    const body = {
        'document' : document,
        'encodingType' : 'UTF8'
    };	

    try {
        const response = await fetch(url + key, {
            method : 'POST',
            body : JSON.stringify(body),
            headers: {'Content-Type' : 'application/json; charset=utf-8'},
        });

        if (response.ok) {
            const json = await response.json();
            const topSalience = json.entities[0];
            const results = {
                'name' : topSalience.name,
                'type' : topSalience.type,
                'salience' : topSalience.salience,
                'entitySentiment' : topSalience.sentiment
            }
            return results;
        }
        else {
            let msg = (`response status: ${response.status}`);
            throw new Error(msg);
        }
    }
    catch (err) {
        let msg = (`entitySentiment() - ${err}`);
        logger.error(msg);
        throw err;
    }
}

/**
 * Main function.  Fetches a twitter bearer token, clears out any existing filter rules, adds
 * new rules, and then sets up a realtime tweet feed for those new rules.
 */
async function filter() {
    logger.debug(`filter()`);
    try {
        symbolArray = await getSymbols(IEX_KEY, SYMBOL_URL);
        const token = await getTwitterToken(AUTH_URL);
        const deleted = await clear(token, RULES_URL);
        logger.info(`Number of rules deleted: ${deleted}`);
        const added = await setRules(token, RULES_URL, RULES);
        logger.info(`Number of rules added: ${added}`);
        await stream(token, STREAM_URL);
    }
    catch(err) {
        logger.error(`filter() exiting`);
        if (g_reader) {
            g_reader.removeAllListeners();
        }
        process.exit(-1);
    }
}

/**
 * Function that fetches the previous day pricing info for a given stock symbol
 * @param {string} token - IEX API token
 * @param {string} url - url to the IEX stock Rest endpoint
 * @param {string} symbol - stock symbol
 * @return {promise} previous day info
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function getPrevious(token, url, symbol) {
    logger.debug(`getPrevious() symbol:${symbol}`);
    
    url += symbol + '/previous' + '?token=' + token;
    try {
        const response = await fetch(url, {
            method: 'GET',
        });
        if (response.ok) {
            const json = await response.json();
            let previous;
            if (json) {
                previous = {
                    'date' : json.date,
                    'open' : json.open,
                    'close' : json.close,
                    'high' : json.high,
                    'low' : json.low
                }
            }
            return previous;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        logger.error(`getPrevious() - ${err}`);
        throw err;
    }
}

/**
 * Function that fetches the current price for a given stock symbol
 * @param {string} token - IEX API token
 * @param {string} url - url to the IEX stock Rest endpoint
 * @param {string} symbol - stock symbol
 * @return {promise} current price
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function getPrice(token, url, symbol) {
    logger.debug(`getPrice() symbol:${symbol}`);
    
    url += symbol + '/quote/latestPrice' + '?token=' + token;
    
    try {
        const response = await fetch(url, {
            method: 'GET',
        });
        if (response.ok) {
            const price = await response.json();
            return price;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        logger.error(`getPrice() - ${err}`);
        throw err;
    }
}

/**
 * Function that fetches the existing set of filter rules
 * @param {string} token - twitter bearer token
 * @param {string} url - url to the twitter filter rules API
 * @return {promise} json object containing an array of existing filter rule ids
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function getRules(token, url) {
    logger.debug(`getRules()`);
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: {
            'Authorization' : 'Bearer ' + token
            }
        });
        if (response.ok) {
            const json = await response.json();
            return json;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        logger.error(`getRules() - ${err}`);
        throw err;
    }
}

/**
 * Function that fetches all existing stock names + symbols
 * @param {string} token - IEX API token
 * @param {string} url - url to the IEX stock Rest endpoint
 * @param {string} name - textual name of the company
 * @return {promise} if the name can be resolved to a stock symbol (publicly traded company), returns
 * the symbol + current and previous day pricing information.  Returns null if the name can't be resolved.
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function getStockData(token, url, symbol) {
    logger.debug(`getStockData() - name:${symbol}`);
    
    let data = {};
    const price = await getPrice(token, url, symbol);
    const previous = await getPrevious(token, url, symbol);
    data.current_price = price;
    data.date = previous.date;
    data.open = previous.open;
    data.close = previous.close;
    data.high = previous.high;
    data.low = previous.low
    return data;
}

/**
 * Function that fetches all existing stock names + symbols
 * @param {string} token - IEX API token
 * @param {string} url - url to the IEX stock symbol Rest endpoint
 * @return {promise} json object containing an array of stock symbol objects
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function getSymbols(token, url) {
    logger.debug(`getSymbols()`);
    
    try {
        const response = await fetch(url + token, {
            method: 'GET',
        });
        if (response.ok) {
            const arr = await response.json();
            return arr;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        logger.error(`getSymbols() - ${err}`);
        throw err;
    }
}

/**
* Fetches an app-only bearer token via Twitter's oauth2 interface
* @param {string} url- URL to Twitter's OAuth2 interface
* @return {string} - Bearer token
*/
async function getTwitterToken(url) {
    logger.debug(`getTwitterToken()`);
    const consumerToken = btoa(urlEncode(TWITTER_KEY) + ':' + urlEncode(TWITTER_SECRET));

    let response, json;

    try {
        response = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization' : 'Basic ' + consumerToken,
                'Content-Type' : 'application/x-www-form-urlencoded;charset=UTF-8'
            }, 
            body : 'grant_type=client_credentials'
        });

        if (response.ok) {
            json = await response.json();
            return json.access_token;
        }
        else {
            throw new Error(`response status: ${response.status}`);
        }
    }
    catch (err) {
        logger.error(`getTwitterToken() - ${err}`)
        throw err;
    } 
} 

/**
 * Function for analyzing entity and sentiment content of a tweet.  If tweet represents a publicly traded company,
 * functions sends a trade alert via email.
 * @param {string} tweet - text of a tweet
 * @return none
 * @throws None
 */
async function processTweet(tweet) {
    logger.debug(`processTweet()`);
    try {
        const esnt = await entitySentiment(GOOGLE_KEY, ENTITY_SENTIMENT_URL, tweet);

        if (esnt.type === 'ORGANIZATION') { //entity corresponds to a type that might be a company
            let stock;
            if (Array.isArray(symbolArray)) {
                stock = symbolArray.find(obj => {
                    return obj.name.match(esnt.name);
                });
                if (stock) {  //name corresponds to a publicly traded company - fetch full tweet sentiment
                    //and stock data
                    const snt = await sentiment(GOOGLE_KEY, SENTIMENT_URL, tweet);
                    const data = await getStockData(IEX_KEY, STOCK_URL, stock.symbol);

                    let analytics = {};
                    analytics.tweet = tweet;
                    analytics.name = esnt.name;
                    analytics.salience = esnt.salience;
                    analytics.entitySentiment = esnt.entitySentiment;
                    analytics.documentSentiment = snt;
                    let mag = (analytics.entitySentiment.magnitude + analytics.documentSentiment.magnitude) / 2;
                    let score = (analytics.entitySentiment.score + analytics.documentSentiment.score) / 2;
                    analytics.aggregate = mag * score;
                    analytics.symbol = stock.symbol;
                    analytics.data = data;
                    sendEmail(SENDGRID_KEY, SENDGRID_URL, analytics);
                }
            }
        }
    }
    catch(err) {
        logger.error(err);
    }
}

/**
 * Function that calls SendGrid's REST endpoint to send an email with the tweet and analytics
 * @param {string} token - SendGrid API token
 * @param {string} url - SendGrid Rest endpoint
 * @param {string} analytics- Google NL + IEX analytics info on the tweet
 * @return none
 * @throws none
 */
async function sendEmail(token, url, analytics) {
    logger.debug(`sendEmail()`);

    try {
        let subject = 'Twitter Trade Alert -'
        if (analytics.aggregate < 0) {
            subject += ' Negative Tweet: ';
        }
        else if (analytics.aggregate > 0) {
            subject += ' Positive Tweet: ';
        }
        else
            subject += ' Neutral Tweet: ';
        subject += analytics.name;

        let recipients = await fsp.readFile(RECIPIENT_FILE);
        recipients = JSON.parse(recipients);

        const personalizations = [
            {
                'to' : recipients,
                'subject' : subject
            }
        ];
        const content = [
            {
                'type' : 'text/plain',
                'value' : JSON.stringify(analytics, null, 4)
            }
        ];
        const from = {'email' : 'twitterTrade@example.com'}

        const body = {
            'personalizations' : personalizations,
            'content' : content,
            'from' : from
        };	

        const response = await fetch(url, {
            method : 'POST',
            body : JSON.stringify(body),
            headers: {
                'Content-Type' : 'application/json',
                'Authorization' : 'Bearer ' + token
            }
        });

        if (response.ok) {
            logger.info(`Email sent, subject: ${subject}`);
            return;
        }
        else {
            let msg = (`response status: ${response.status}`);
            throw new Error(msg);
        }
    }
    catch (err) {
        let msg = (`sendEmail() - ${err}`);
        logger.error(msg)
    }
}

/**
 * Function that calls Google Natural Language sentiment REST end point of the entire tweet
 * @param {string} key - Google API key
 * @param {string} url - Google NL Rest endpoint
 * @param {string} tweet - text of a tweet
 * @return {promise} JSON object containing sentiment analysis
 * @throws {Error} propagates HTTP status errors or node fetch exceptions
 */
async function sentiment(key, url, tweet) {
    logger.debug(`sentiment()`);

    const document = {
        'type': 'PLAIN_TEXT',
        'language': 'en',
        'content': tweet
    };

    const body = {
        'document' : document,
        'encodingType' : 'UTF8'
    };	

    try {
        const response = await fetch(url + key, {
            method : 'POST',
            body : JSON.stringify(body),
            headers: {'Content-Type' : 'application/json; charset=utf-8'},
        });

        if (response.ok) {
            const json = await response.json();
            return json.documentSentiment;
        }
        else {
            let msg = (`response status: ${response.status}`);
            throw new Error(msg);
        }
    }
    catch (err) {
        let msg = (`sentiment() - ${err}`);
        logger.error(msg)
        throw err;
    }
}

/**
 * Function that adds filter rules to an account
 * @param {string} token - twitter bearer token
 * @param {string} url - url to the twitter filter rules API
  * @param {Array} rules - array of filter rules to add
 * @return {promise} integer representing number of rules added
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function setRules(token, url, rules) {
    logger.debug(`setRules()`);
 
    const body = {'add' : rules};
    try {
        const response = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type'  : 'application/json',
                'Authorization' : 'Bearer ' + token
            },
            body: JSON.stringify(body)
        });
        if (response.ok) {
            const json = await response.json();
            return json.meta.summary.created;
        }
        else {
            throw new Error(`response status: ${response.status} ${response.statusText}`);    
        }
    }
    catch (err) {
        logger.error(`setRules() - ${err}`);
        throw err;
    }
}

/**
 * Function that performs a call to the twitter filter search API and displays tweets in realtime.
 * @param {string} token - twitter bearer token
 * @param {string} url - url to the twitter stream API
 * @return {promise} none
 * @throws {Error} propagates HTTP status errors or node-fetch exceptions
 */
async function stream(token, url) {
    logger.debug(`stream()`);

    const controller = new AbortController();
    let abortTimer = setTimeout(() => { controller.abort(); }, ABORT_TIMEOUT * 1000);
    
    try {
        const response = await fetch(url, {
            method: 'GET',
            headers: {
            'Authorization' : 'Bearer ' + token
            },
            signal: controller.signal 
        });
        switch (response.status) {
            case 200:
                logger.debug(`stream() - 200 response`);
                g_reader = response.body;
                g_reader.on('data', (chunk) => {
                    try {
                        const json = JSON.parse(chunk);
                        let tweet = json.data.text.replace(/\r?\n|\r|@|#/g, ' ');  //remove newlines, @ and # from tweet text
                        processTweet(tweet);
                    }
                    catch (err) {
                        //heartbeat will generate a json parse error.  No action necessary; continue to read the stream.
                        //logger.debug(`stream() - heartbeat received`);
                    } 
                    finally {
                        g_backoff = 0;
                        clearTimeout(abortTimer);
                        abortTimer = setTimeout(() => { controller.abort(); }, ABORT_TIMEOUT * 1000);
                    } 
                });
                g_reader.on('error', err => {  //self-induced timeout, immediately reconnect
                    if (err.name === 'AbortError') {
                        logger.warn(`stream() - connection aborted, reconnecting`);
                        g_backoff = 0;
                        clearTimeout(abortTimer);
                        (async () => {await stream(token, url)}) ();
                    }
                    else if (err.code === 'ETIMEDOUT') { //back off 250 ms linearly to 16 sec for connection timeouts
                        logger.warn(`stream() - connection timeout, reconnecting`);
                        g_backoff += .25; 
                        if (g_backoff > 16) {
                            g_backoff = 16;
                        }
                        clearTimeout(abortTimer); 
                        setTimeout(async () => {await stream(token, url)}, g_backoff * 1000);
                    }
                    else {  //fatal error, bail
                        logger.error(`stream() - fatal error ${err}`);
                        clearTimeout(abortTimer);
                        g_reader.removeAllListeners();
                        process.exit(-1);
                    }
                });
                break;
            case 304:  //60 second backoff
                logger.debug(`stream() - 304 response`);
                g_backoff = 60;
                clearTimeout(abortTimer);
                setTimeout(async () => {await stream(token, url)}, g_backoff * 1000);
                break;
            case 420:
            case 429:  //exponential backoff, starting at 60 sec, for rate limit errors
                logger.warn(`stream() - 420, 429 response`);
                if (g_backoff == 0) {
                    g_backoff = 60;
                }
                else {
                    g_backoff = g_backoff * 2;
                }
                clearTimeout(abortTimer);
                setTimeout(async () => {await stream(token, url)}, g_backoff * 1000);
                break;
            case 500:
            case 502:
            case 503:
            case 504: //exponential backoff, starting at 5 sec, for server side errors
                logger.warn(`stream() - 5xx response`);
                if (g_backoff == 0) {
                    g_backoff = 5;
                }
                else {
                    g_backoff *= 2;
                    if (g_backoff > 320) {
                        g_backoff = 320;
                    }
                }
                clearTimeout(abortTimer);  
                setTimeout(async () => {await stream(token, url)}, g_backoff * 1000);
                break;    
            default:  //fail fast for any other errors (4xx client errors, for example)
                clearTimeout(abortTimer);
                g_reader.removeAllListeners();
                throw new Error(`response status: ${response.status}`);
        }
        return;
    }
    catch (err) { //fatal error
        clearTimeout(abortTimer);
        g_reader.removeAllListeners();
        logger.error(`stream() - ${err}`);
        throw err;
    }
}

/**
* Function I found on stackoverflow to provide url encoding
* @param {string} str- string to be encoded
* @return {string} - url encoded string
*/
function urlEncode (str) {
    return encodeURIComponent(str)
        .replace(/!/g, '%21')
        .replace(/'/g, '%27')
        .replace(/\(/g, '%28')
        .replace(/\)/g, '%29')
        .replace(/\*/g, '%2A')
}


filter()
.catch(err => {
    logger.error(err);
    g_reader.removeAllListeners();
    process.exit(-1);
});


/*
async function test() {
    symbolArray = await getSymbols(IEX_KEY, SYMBOL_URL);
    const tweet1 = "Surprised that Harley-Davidson, of all companies, would be the first to wave the White Flag. I fought hard for them and ultimately they will not pay tariffs selling into the E.U., which has hurt us badly on trade, down $151 Billion. Taxes just a Harley excuse - be patient!";
    await processTweet(tweet1);
}

test()
.then(() => {
    console.log('complete');
});
*/
