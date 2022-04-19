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

# This is an integration assignment,
# so the specifications for receiving orders are exactly the same as in Exchange III
# and the specifications for matching orders are exactly the same as in Exchange II.

""" Suggested helper methods """


def process_order(order):

    # Accept orders and store them in database first
    # committed_order_obj = commit_new_order(order)
    
    g.session.add(order)
    g.session.commit()
    
    
    
    # Match with existing order
    existing_order_obj = match_order(order)

    if(existing_order_obj is not None):
        new_order_for_remaining = commit_derived_order_obj(
            order, existing_order_obj)
        if(new_order_for_remaining is not None):
            process_order(new_order_for_remaining)

    else:
        return


# def commit_new_order(new_order):

#     # The order book should accept orders and store them in database first
#     committed_order_obj = Order(sender_pk=new_order['sender_pk'], receiver_pk=new_order['receiver_pk'], buy_currency=new_order['buy_currency'],
#                                 sell_currency=new_order['sell_currency'], buy_amount=new_order['buy_amount'], sell_amount=new_order['sell_amount'])
#     g.session.add(committed_order_obj)
#     g.session.commit()

#     return committed_order_obj


def match_order(new_order):

    # existing_order.filled must be None
    # existing_order.buy_currency == order.sell_currency
    # existing_order.sell_currency == order.buy_currency
    # The implied exchange rate of the new order must be at least that of the existing order
    # The buy / sell amounts need not match exactly
    # Each order should match at most one other

    existing_order_obj = g.session.query(Order).filter(Order.filled == None, Order.sell_currency == new_order.buy_currency, Order.buy_currency == new_order.sell_currency,
                                                       ((Order.sell_amount / Order.buy_amount) >=
                                                        (new_order.buy_amount / new_order.sell_amount)),
                                                       Order.sell_amount != Order.buy_amount, new_order.buy_amount != new_order.sell_amount)
    return existing_order_obj.first()


def commit_derived_order_obj(committed_order_obj, existing_order_obj):

    # Set the filled field to be the current timestamp on both orders
    committed_order_obj.filled = datetime.now()
    existing_order_obj.filled = datetime.now()

    # Set counterparty_id to be the id of the other order
    committed_order_obj.counterparty_id = existing_order_obj.id
    existing_order_obj.counterparty_id = committed_order_obj.id

    # If one of the orders is not completely filled
    if committed_order_obj.buy_amount > existing_order_obj.sell_amount:

        # Create a new order for remaining balance
        remaining_balance = committed_order_obj.buy_amount - existing_order_obj.sell_amount
        ex_rate = committed_order_obj.buy_amount / committed_order_obj.sell_amount

        derived_order_obj = Order(creator_id=committed_order_obj.id, sender_pk=committed_order_obj.sender_pk, receiver_pk=committed_order_obj.receiver_pk, buy_currency=committed_order_obj.buy_currency,
                                  sell_currency=committed_order_obj.sell_currency, buy_amount=remaining_balance, sell_amount=remaining_balance / ex_rate)
        g.session.add(derived_order_obj)
        g.session.commit()

    elif committed_order_obj.buy_amount < existing_order_obj.sell_amount:

        # Create a new order for remaining balance
        remaining_balance = existing_order_obj.sell_amount - committed_order_obj.buy_amount
        ex_rate = existing_order_obj.sell_amount / existing_order_obj.buy_amount

        derived_order_obj = Order(creator_id=existing_order_obj.id, sender_pk=existing_order_obj.sender_pk, receiver_pk=existing_order_obj.receiver_pk, buy_currency=existing_order_obj.buy_currency,
                                  sell_currency=existing_order_obj.sell_currency, buy_amount=remaining_balance / ex_rate, sell_amount=remaining_balance)
        g.session.add(derived_order_obj)
        g.session.commit()

    else:
        g.session.commit()


def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    # Takes input dictionary d and writes it to the Log table
    g.session.add(Log(logtime=datetime.now(),message=json.dumps(d)))
    g.session.commit()


def appendOrder(order, data):
    data.append({
        'sender_pk': order.sender_pk,
        'receiver_pk': order.receiver_pk,
        'buy_currency': order.buy_currency,
        'sell_currency': order.sell_currency,
        'buy_amount': order.buy_amount,
        'sell_amount': order.sell_amount,
        'signature': order.signature
    })


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

        # The platform must be either “Algorand” or "Ethereum". Your code should check whether “sig” is a valid signature of json.dumps(payload), using the signature algorithm specified by the platform field. Be sure to sign the payload using the sender_pk.
        # If the signature verifies, all of the fields under the ‘payload’ key should be stored in the “Order” table EXCEPT for 'platform’.
        # If the signature does not verify, do not insert the order into the “Order” table.
        # TODO: Check the signature
        if platform == 'Algorand':
            if algosdk.util.verify_bytes(payload.encode('utf-8'), signature, sender_public_key):
                process_order(Order(sender_pk=sender_public_key, receiver_pk=receiver_public_key,
                              buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount, signature=signature))
                return jsonify(True)
            else:
                log_message(content)
                return jsonify(False)

        # TODO: Check the signature
        elif platform == 'Ethereum':
            eth_encoded_msg = eth_account.messages.encode_defunct(text=payload)
            if eth_account.Account.recover_message(eth_encoded_msg, signature=signature) == sender_public_key:
                process_order(Order(sender_pk=sender_public_key, receiver_pk=receiver_public_key,
                              buy_currency=buy_currency, sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount, signature=signature))
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
        appendOrder(order, data)
        # print(order)
    return jsonify(data=data)


if __name__ == '__main__':
    app.run(port='5002')
