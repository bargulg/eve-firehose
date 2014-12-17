#!/usr/bin/env python2
import sys
import time
import logging
import calendar
import traceback
import elasticsearch
import dateutil.parser


# need to disable WARN level logging cause of ES 404 errors lol
logging.basicConfig(level=50)


class MarketMessageProcessor():
    def __init__(self):
        self.es = elasticsearch.Elasticsearch()
        self.processed = 0

    def fail(self, what, reason):
        return {"success": False, "type": what, "reason": reason}

    def parse_row(self, cols, row):
        return dict(zip(cols,row))

    def is_datestring(self, datestring):
        try:
            dateutil.parser.parse(datestring)
            return True
        except ValueError:
            return False

    def datestring2timestamp(self, datestring):
        ret = dateutil.parser.parse(datestring).timetuple()
        ret = calendar.timegm(ret)
        ret = int(ret)
        return ret

    def check_orders_msg(self, message):
        required_cols = ['price', 'volRemaining', 'range', 'orderID', 'volEntered',
                         'minVolume', 'bid', 'issueDate', 'duration', 'stationID',
                         'solarSystemID']

        floatCols = ['price']
        intCols = ['volRemaining', 'range', 'orderID', 'volEntered',
                   'minVolume', 'duration', 'stationID', 'solarSystemID']
        boolCols = ['bid']
        dateCols = ['issueDate']

        assert "columns" in message
        assert sorted(message['columns']) == sorted(required_cols)

        assert "rowsets" in message

        for rowset in message["rowsets"]:
            assert "generatedAt" in rowset
            assert "typeID" in rowset
            assert "regionID" in rowset
            assert "rows" in rowset

            assert self.is_datestring(rowset["generatedAt"])
            generatedAt = self.datestring2timestamp(rowset["generatedAt"])

            for row in rowset["rows"]:
                assert len(row) == len(required_cols)

                order = self.parse_row(message["columns"], row)

                for col in floatCols:
                    assert type(order[col]) == float
                for col in intCols:
                    assert type(order[col]) == int
                for col in boolCols:
                    assert type(order[col]) == bool
                for col in dateCols:
                    assert self.is_datestring(order[col])

                assert self.is_datestring(order["issueDate"])
                issueDate = self.datestring2timestamp(order["issueDate"])

                assert order["price"] > 0.0
                assert order["volRemaining"] > 0
                assert order["range"] >= -1
                assert order["orderID"] >= 0
                assert order["volEntered"] > 0
                assert order["minVolume"] > 0
                assert issueDate <= generatedAt
                assert order["duration"] > 0
                assert order["stationID"] > 0
                assert order["solarSystemID"] > 0

    def process_orders(self, message):
        # be paranoid
        try:
            self.check_orders_msg(message)
        except:
            failedLine = traceback.extract_tb(sys.exc_info()[2])[-1][3]
            return self.fail("orders", "check failed: " + failedLine)

        numOrders = 0

        # rowset contains orders and metadata for a single typeID,
        # message can contain multiple rowsets
        for rowset in message["rowsets"]:
            typeID = rowset['typeID']
            regionID = rowset['regionID']
            processedAt = int(time.time())
            generatedAt = self.datestring2timestamp(rowset["generatedAt"])

            # data for removal of fulfilled/cancelled orders
            activeIDs = set()

            # row contains data for a single order
            for row in rowset["rows"]:
                numOrders += 1

                o = self.parse_row(message["columns"], row)
                o["issueDate"] = self.datestring2timestamp(o["issueDate"])

                orderID = o['orderID']
                activeIDs.add(orderID)
                ttl = o['issueDate'] + o['duration']*24*60*60
                order_data = {'typeID': typeID,
                              'regionID': regionID,
                              'generatedAt': generatedAt,
                              'processedAt': processedAt,
                              'price': o['price'],
                              'volRemaining': o['volRemaining'],
                              'range': o['range'],
                              'volEntered': o['volEntered'],
                              'minVolume': o['minVolume'],
                              'bid': o['bid'],
                              'issueDate': o['issueDate'],
                              'duration': o['duration'],
                              'stationID': o['stationID'],
                              'probablyOld': False}

                # check whether the received data is in fact new
                try:
                    oldData = self.es.get_source(index="emdr", doc_type="order",
                                                id=orderID)
                    if oldData["generatedAt"] >= generatedAt \
                            or oldData["volRemaining"] < o['volRemaining']:
                        # reveived stale data, fayul
                        continue
                except elasticsearch.exceptions.NotFoundError:
                    # no record found in DB is fine, continue
                    pass
                except:
                    # something failed hard
                    return self.fail("order", "can't read from elasticsearch db: "
                                     + str(sys.exc_info()[1]))

                # insert new/updated order data into DB
                try:
                    self.es.index(index="emdr", doc_type="order", id=orderID,
                                  body=order_data, timestamp=o['issueDate'],
                                  ttl=ttl)
                except:
                    # something failed hard
                    return self.fail("orders", "can't insert into elasticsearch db: "
                                     + str(sys.exc_info()[1]))

            # now try to mark fulfilled/cancelled orders
            # 1) select all order IDs for given typeID/regionID pair
            query = {
                "filtered": {
                    "filter": {
                        "and": [
                            {"term": {"typeID": typeID}},
                            {"term": {"regionID": regionID}},
                        ]
                    }
                }
            }
            allIDs = set()
            # start the scan search scroller (no results returned here)
            scroller = self.es.search(index="emdr", doc_type="order",
                                      body={"query": query},
                                      _source=False,
                                      scroll="1m",
                                      search_type="scan")
            scrollID = scroller['_scroll_id']
            while True:
                ret = self.es.scroll(scroll_id=scrollID, scroll="1m")
                scrollID = ret['_scroll_id']
                moreIDs = [int(a['_id']) for a in ret['hits']['hits']]
                if moreIDs == []:
                    break
                allIDs.update(set(moreIDs))
            # 2) remove activeIDs from the set
            inactiveIDs = list(allIDs.difference(activeIDs))
            # 3) mark the remaining orders as probably old
            for orderID in inactiveIDs:
                try:
                    oldData = self.es.get_source(index="emdr",
                                                 doc_type="order",
                                                 id=orderID)
                    oldData['probablyOld'] = True
                    ttl = oldData['issueDate'] + oldData['duration']*24*60*60
                    self.es.index(index="emdr", doc_type="order", id=orderID,
                                  body=oldData,
                                  timestamp=oldData['issueDate'],
                                  ttl=ttl)
                except elasticsearch.exceptions.NotFoundError:
                    # probably ttl expired already
                    pass
                except:
                    # something failed hard
                    return self.fail("order", "can't read from elasticsearch db: "
                                     + str(sys.exc_info()[1]))


        return {"success": True, "type": "orders", "number": numOrders}

    def check_history_msg(self, message):
        required_cols = ['date', 'orders', 'quantity', 'low', 'high', 'average']

        dateCols = ['date']
        intCols = ['orders', 'quantity']
        floatCols = ['low', 'high', 'average']

        assert "columns" in message
        assert len(message["columns"]) == 6

        for col in required_cols:
            assert col in message["columns"]

        assert "rowsets" in message

        for rowset in message["rowsets"]:
            assert "generatedAt" in rowset
            assert "typeID" in rowset
            assert "regionID" in rowset
            assert "rows" in rowset

            assert self.is_datestring(rowset["generatedAt"])
            generatedAt = self.datestring2timestamp(rowset["generatedAt"])

            for row in rowset["rows"]:
                assert len(row) == len(required_cols)

                history = self.parse_row(message["columns"], row)

                for col in dateCols:
                    assert self.is_datestring(history[col])
                for col in intCols:
                    assert type(history[col]) == int
                for col in floatCols:
                    assert type(history[col]) == float

                assert self.is_datestring(history["date"])
                date = self.datestring2timestamp(history["date"])

                assert date <= generatedAt
                assert history["orders"] >= 0
                assert history["quantity"] >= 0
                assert history["low"] > 0.0
                assert history["high"] >= history["low"]
                assert history["average"] >= history["low"]
                assert history["average"] <= history["high"]

    def process_history(self, message):
        raise NotImplementedError
