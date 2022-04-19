from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

@app.before_request
def create_session():
    g.session = scoped_session(DBSession)

@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """
def attachList(order, data):
    data.append({
        'sender_pk': order.sender_pk,
        'receiver_pk': order.receiver_pk,
        'buy_currency': order.buy_currency,
        'sell_currency': order.sell_currency,
        'buy_amount': order.buy_amount,
        'sell_amount': order.sell_amount,
        'signature': order.signature
    })

def startBuys(order):

    g.session.add(order)
    g.session.commit()
    est_o = Fillbuys(order)

    if (est_o is not None):
        left_account = setBuys(order, est_o)
        if (left_account is not None):
            startBuys(left_account)

    else:
        return

def Fillbuys(new_order):
    est_o = g.session.query(Order).filter(Order.filled == None, Order.sell_currency == new_order.buy_currency, Order.buy_currency == new_order.sell_currency,((Order.sell_amount / Order.buy_amount) >= (new_order.buy_amount / new_order.sell_amount)), Order.sell_amount != Order.buy_amount, new_order.buy_amount != new_order.sell_amount)

    return est_o.first()


def setBuys(com_o, est_o):
    com_o.filled = datetime.now()
    est_o.filled = datetime.now()

    com_o.counterparty_id = est_o.id
    est_o.counterparty_id = com_o.id

    if com_o.buy_amount > est_o.sell_amount:

        account_left = com_o.buy_amount - est_o.sell_amount
        exchange = com_o.buy_amount / com_o.sell_amount

        dev_o = Order(creator_id=com_o.id, sender_pk=com_o.sender_pk,
                                  receiver_pk=com_o.receiver_pk,
                                  buy_currency=com_o.buy_currency,
                                  sell_currency=com_o.sell_currency, buy_amount=account_left,
                                  sell_amount=account_left / exchange)
        g.session.add(dev_o)
        g.session.commit()

    elif com_o.buy_amount < est_o.sell_amount:

        # Create a new order for remaining balance
        account_left = est_o.sell_amount - com_o.buy_amount
        exchange = est_o.sell_amount / est_o.buy_amount

        dev_o = Order(creator_id=est_o.id, sender_pk=est_o.sender_pk,
                                  receiver_pk=est_o.receiver_pk,
                                  buy_currency=est_o.buy_currency,
                                  sell_currency=est_o.sell_currency,
                                  buy_amount= account_left / exchange, sell_amount=account_left)
        g.session.add(dev_o)
        g.session.commit()

    else:
        g.session.commit()


def log_message(d):
    g.session.add(Log(logtime=datetime.now(), message=json.dumps(d)))
    g.session.commit()

""" End of helper methods """


@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print(f"content = {json.dumps(content)}")
        columns = ["sender_pk", "receiver_pk", "buy_currency",
                   "sell_currency", "buy_amount", "sell_amount", "platform"]
        fields = ["sig", "payload"]

        for field in fields:
            if not field in content.keys():
                print(f"{field} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        error = False
        for column in columns:
            if not column in content['payload'].keys():
                print(f"{column} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        if error:
            print(json.dumps(content))
            log_message(content)
            return jsonify(False)

        # Your code here
        # Note that you can access the database session using g.session
        signature = content['sig']
        payload = json.dumps(content['payload'])
        sender_public_key = content['payload']['sender_pk']
        receiver_public_key = content['payload']['receiver_pk']
        buy_currency = content['payload']['buy_currency']
        sell_currency = content['payload']['sell_currency']
        buy_amount = content['payload']['buy_amount']
        sell_amount = content['payload']['sell_amount']
        platform = content['payload']['platform']

        # TODO: Check the signature
        if platform == 'Algorand':
            if algosdk.util.verify_bytes(payload.encode('utf-8'), signature, sender_public_key):
                startBuys(Order(sender_pk=sender_public_key, receiver_pk=receiver_public_key,
                                    buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount,
                                    sell_amount=sell_amount, signature=signature))
                return jsonify(True)
            else:
                log_message(content)
                return jsonify(False)

        # TODO: Check the signature
        elif platform == 'Ethereum':
            e_msg = eth_account.messages.encode_defunct(text=payload)
            if eth_account.Account.recover_message(e_msg, signature=signature) == sender_public_key:
                startBuys(Order(sender_pk=sender_public_key, receiver_pk=receiver_public_key,
                                    buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount,
                                    sell_amount=sell_amount, signature=signature))
                return jsonify(True)
            else:
                log_message(content)
                return jsonify(False)


@app.route('/order_book')
def order_book():
    # Your code here
    # Note that you can access the database session using g.session
    data = []
    for order in g.session.query(Order).all():
        attachList(order, data)
    return jsonify(data=data)


if __name__ == '__main__':
    app.run(port='5002')
