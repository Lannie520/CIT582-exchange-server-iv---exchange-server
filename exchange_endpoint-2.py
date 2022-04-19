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
def check_order(new_o):

    est_o = g.session.query(Order).filter(Order.filled == None, Order.sell_currency == new_o.buy_currency, Order.buy_currency == new_o.sell_currency,((Order.sell_amount / Order.buy_amount) >= (new_o.buy_amount / new_o.sell_amount)), Order.sell_amount != Order.buy_amount, new_o.buy_amount != new_o.sell_amount)
    return est_o.first()

def check_sig(payload,sig):
    # Set the timestamps
    payload.filled = datetime.now()
    sig.filled = datetime.now()

    # The exchange the id
    payload.counterparty_id = sig.id
    sig.counterparty_id = payload.id

    # The condition that the order is not filled
    if payload.buy_amount > sig.sell_amount:

        # Set a new one in the account
        account_left = payload.buy_amount - sig.sell_amount
        exchange = payload.buy_amount / payload.sell_amount

        dev_o = Order(creator_id=payload.id, sender_pk=payload.sender_pk,receiver_pk=payload.receiver_pk, buy_currency=payload.buy_currency, sell_currency= payload.sell_currency, buy_amount=account_left, sell_amount= account_left / exchange)
        g.session.add(dev_o)
        g.session.commit()

    elif payload.buy_amount < sig.sell_amount:

        # Generate a new for the left things
        account_left = sig.sell_amount - payload.buy_amount
        exchange = sig.sell_amount / sig.buy_amount

        dev_o = Order(creator_id=sig.id, sender_pk=sig.sender_pk, receiver_pk=sig.receiver_pk, buy_currency=sig.buy_currency, sell_currency=sig.sell_currency, buy_amount=account_left / exchange, sell_amount=account_left)
        g.session.add(dev_o)
        g.session.commit()

    else:
        g.session.commit()


def fill_order(order):
    g.session.add(order)
    g.session.commit()

    # The orders right now can be correspondent
    est_o = check_order(order)

    if (est_o is not None):
        new_o = check_sig(order, est_0)
        if (new_o is not None):
            fill_order(new_o)

    else:
        return
  
def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    g.session.add(Log(logtime=datetime.now(), message=json.dumps(d)))
    g.session.commit()

""" End of helper methods """



@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print( f"content = {json.dumps(content)}" )
        columns = [ "sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform" ]
        fields = [ "sig", "payload" ]

        for field in fields:
            if not field in content.keys():
                print( f"{field} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
        
        for column in columns:
            if not column in content['payload'].keys():
                print( f"{column} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
            
        #Your code here
        #Note that you can access the database session using g.session
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
                fill_order(Order(sender_pk=sender_public_key, receiver_pk=receiver_public_key, buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount, signature=signature))
                return jsonify(True)
            else:
                log_message(content)
                return jsonify(False)

        elif platform == 'Ethereum':
            e_msg = eth_account.messages.encode_defunct(text=payload)
            if eth_account.Account.recover_message(e_msg, signature=signature) == sender_public_key:
                fill_order(Order(sender_pk=sender_public_key, receiver_pk=receiver_public_key, buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount, signature=signature))
                return jsonify(True)
            else:
                log_message(content)
                return jsonify(False)
        # TODO: Add the order to the database
        
        # TODO: Fill the order
        
        # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful
        

@app.route('/order_book')
def order_book():
    #Your code here
    #Note that you can access the database session using g.session
    data = []
    for order in g.session.query(Order).all():
        attachList(order, data)
    return jsonify(data=data)

if __name__ == '__main__':
    app.run(port='5002')