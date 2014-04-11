#!/usr/bin/env python2
import sys
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

    def fail(self, what, reason):
        return {"success": False, "type": what, "reason": reason}

    def parse_row(self, cols, row):
        ret = {}
        for i in range(len(cols)):
            ret[cols[i]] = row[i]
        return ret

    def is_datestring(self, datestring):
        try:
            dateutil.parser.parse(datestring).timetuple()
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
        assert len(message["columns"]) == len(required_cols)

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
            generatedAt = self.datestring2timestamp(rowset["generatedAt"])

            itemData = {"typeID": rowset["typeID"],
                        "regionID": rowset["regionID"],
                        "generatedAt": generatedAt,
                        "orders": []}

            # construct custom type-region ID
            constructedID = str(itemData["typeID"]) + "-" + str(itemData["regionID"])

            # check whether the received data is in fact new
            try:
                oldData = self.es.get(index="orders", doc_type="item-region",
                                      id=constructedID)
                if oldData["_source"]["generatedAt"] > generatedAt:
                    # received stale data, fayul
                    return self.fail("orders", "stale data")
            except elasticsearch.exceptions.NotFoundError:
                # no record found in DB is fine, continue
                pass
            except:
                # something failed hard
                return self.fail("orders", "can't read from elasticsearch db: "
                                 + str(sys.exc_info()[1]))

            # row contains data for a single order
            for row in rowset["rows"]:
                order = self.parse_row(message["columns"], row)
                order["issueDate"] = self.datestring2timestamp(order["issueDate"])
                itemData["orders"].append(order)
                numOrders += 1

            # insert new/updated order data into DB
            try:
                self.es.index(index="orders", doc_type="item-region", id=constructedID,
                              body=itemData, timestamp=generatedAt)
            except:
                # something failed hard
                return self.fail("orders", "can't insert into elasticsearch db: "
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
        try:
            self.check_history_msg(message)
        except:
            failedLine = traceback.extract_tb(sys.exc_info()[2])[-1][3]
            self.fail("history", "check failed: " + str(failedLine))

        numHistory = 0

        # rowset contains history and metadata for a single typeID,
        # message can contain multiple rowsets
        for rowset in message["rowsets"]:
            generatedAt = self.datestring2timestamp(rowset["generatedAt"])

            itemData = {"typeID": rowset["typeID"],
                        "regionID": rowset["regionID"],
                        "generatedAt": generatedAt,
                        "history": []}

            # construct custom type-region ID
            constructedID = str(itemData["typeID"]) + "-" + str(itemData["regionID"])

            # check whether the received data is in fact new
            try:
                oldData = self.es.get(index="history", doc_type="item-region",
                                      id=constructedID)
                if oldData["_source"]["generatedAt"] > generatedAt:
                    # reveived stale data, fayul
                    return self.fail("history", "stale data")
            except elasticsearch.exceptions.NotFoundError:
                # no record found in DB is fine, continue
                pass
            except:
                # something failed hard
                return self.fail("history", "can't read from elasticsearch db: "
                                 + str(sys.exc_info()[1]))

            # row contains history data (one row for each day)
            for row in rowset["rows"]:
                history = self.parse_row(message["columns"], row)
                history["date"] = self.datestring2timestamp(history["date"])
                itemData["history"].append(history)
                numHistory += 1

            # insert new/updated history data into DB
            #XXX; yes, it can overwrite history that can't be changed in reality (NOTABUG)
            try:
                self.es.index(index="history", doc_type="item-region",
                              id=constructedID, body=itemData, timestamp=generatedAt)
            except:
                # something failed hard
                return self.fail("history", "can't insert into elasticsearch db: "
                                 + str(sys.exc_info()[1]))

        return {"success": True, "type": "history", "number": numHistory}
