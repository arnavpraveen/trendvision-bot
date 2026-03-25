#!/usr/bin/env python3

from ib_insync import *
import time
from datetime import datetime

# ==========================================
# CONFIGURATION
# ==========================================
TICKER = "AAPL"
QUANTITY = 1
TRAIL_PERCENT = 2.0   # The stop follows price by 2%
BUFFER_PERCENT = 2.0  # Add 2% to 'Last' price to ensure Buy fill
LIMIT_OFFSET = -0.15  # Negative for SELL: Limit is BELOW Stop
# ==========================================

ib = IB()

def get_last_price(contract):
    print(f"🔍 Fetching reference price for {contract.symbol}...")
    bars = ib.reqHistoricalData(
        contract, endDateTime='', durationStr='1 W',       
        barSizeSetting='1 min', whatToShow='TRADES', useRTH=False, formatDate=1
    )
    if bars:
        return bars[-1].close
    return None

def main():
    try:
        print("\n" + "="*70)
        print("AUTOMATED EXTENDED HOURS TRADER (TRAIL LIMIT)")
        print("="*70)
        
        ib.connect('127.0.0.1', 7497, clientId=337) 
        print("✅ Connected")
        ib.reqMarketDataType(3)

        contract = Stock(TICKER, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        
        price = get_last_price(contract)
        if not price: return

        # -------------------------------------------------
        # STEP 1: BUY ORDER
        # -------------------------------------------------
        limit_price = round(price * (1 + BUFFER_PERCENT/100), 2)
        print("\n" + "-"*30)
        print(" STEP 1: PLACING ENTRY")
        print("-"*30)
        print(f"   Target Limit: ${limit_price:.2f}")

        buy_order = LimitOrder('BUY', QUANTITY, limit_price)
        buy_order.tif = 'DAY' 
        buy_order.outsideRth = True 
        
        buy_trade = ib.placeOrder(contract, buy_order)
        
        while not buy_trade.isDone():
            ib.sleep(1)
            if buy_trade.orderStatus.status == 'Cancelled':
                print("❌ Buy Order Cancelled.")
                return
        
        fill_price = buy_trade.orderStatus.avgFillPrice
        fill_qty = buy_trade.orderStatus.filled
        print(f"✅ FILLED! {fill_qty} shares @ ${fill_price:.2f}")

        # -------------------------------------------------
        # STEP 2: TRAILING STOP LIMIT (FIXED)
        # -------------------------------------------------
        print("\n" + "-"*30)
        print(" STEP 2: PLACING PROTECTIVE STOP")
        print("-"*30)
        
        # 1. Calculate the Initial Stop Price explicitly
        # This fixes Error 321 ("Please enter a stop price")
        initial_stop_price = round(fill_price * (1 - TRAIL_PERCENT/100), 2)

        stop_order = Order()
        stop_order.action = 'SELL'
        stop_order.orderType = 'TRAIL LIMIT'
        stop_order.totalQuantity = fill_qty
        stop_order.tif = 'GTC'
        stop_order.outsideRth = True
        
        stop_order.trailingPercent = TRAIL_PERCENT
        stop_order.trailStopPrice = initial_stop_price  # <--- THE FIX
        stop_order.lmtPriceOffset = LIMIT_OFFSET 
        
        stop_trade = ib.placeOrder(contract, stop_order)
        ib.sleep(2) 

        print(f"✅ Stop Placed (ID: {stop_trade.order.orderId})")
        print(f"   Initial Stop: ${initial_stop_price:.2f}")
        print(f"   Trailing:     {TRAIL_PERCENT}%")
        print(f"   Limit Offset: {LIMIT_OFFSET}")

        # Monitoring
        print("\n👀 MONITORING...")
        while True:
            ib.sleep(5)
            status = stop_trade.orderStatus.status
            
            # Note: trailStopPrice updates in the system, but often reads 0 or MAX in local object
            # until a specific event updates it. We rely on status here.
            print(f"Status: {status}")
            
            if status in ['Filled', 'Cancelled', 'Inactive']:
                print(f"🛑 Trace Finished: {status}")
                break

    except KeyboardInterrupt:
        print("\n👋 User Stopped.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        ib.disconnect()

if __name__ == "__main__":
    main()