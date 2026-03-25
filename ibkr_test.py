# test_ibkr_order.py
from ib_insync import *
import time

# Connect to IBKR
ib = IB()
util.startLoop()

try:
    ib.connect('127.0.0.1', 7497, clientId=2) #7497 for TWS, 4002 for IB Gateway
    print("✅ Connected to IBKR")
    print(f"Account: {ib.managedAccounts()}\n")
    
    # Test settings
    ticker = 'AAPL'
    shares = 1
    
    # Create contract
    contract = Stock(ticker, 'SMART', 'USD')
    ib.qualifyContracts(contract)
    
    print(f"📈 Placing MARKET BUY order: {shares} share of {ticker}")
    
    # MARKET ORDER - will fill immediately at current price
    order = MarketOrder('BUY', shares)
    order.tif = 'GTC'
    order.outsideRth = True
    
    trade = ib.placeOrder(contract, order)
    
    # Wait for fill
    print("⏳ Waiting for fill...")
    timeout = 30
    elapsed = 0
    
    while not trade.isDone() and elapsed < timeout:
        ib.sleep(1)
        elapsed += 1
        if elapsed % 5 == 0:
            print(f"   Status: {trade.orderStatus.status} ({elapsed}s)")
    
    if trade.orderStatus.status == 'Filled':
        fill_price = trade.orderStatus.avgFillPrice
        print(f"\n✅ FILLED at ${fill_price:.2f}")
        
        # Calculate stop price (5% below)
        stop_price = round(fill_price * 0.95, 2)
        trailing_amount = round(fill_price * 0.05, 2)
        
        print(f"\n🛡️ Placing 5% trailing stop...")
        print(f"   Entry: ${fill_price:.2f}")
        print(f"   Initial stop: ${stop_price:.2f}")
        
        # TRAIL LIMIT order
        trailing_stop = Order()
        trailing_stop.action = 'SELL'
        trailing_stop.orderType = 'TRAIL LIMIT'
        trailing_stop.totalQuantity = shares
        trailing_stop.trailStopPrice = stop_price
        trailing_stop.lmtPriceOffset = 0.50
        trailing_stop.auxPrice = trailing_amount
        trailing_stop.tif = 'GTC'
        trailing_stop.outsideRth = True
        
        stop_trade = ib.placeOrder(contract, trailing_stop)
        
        print(f"✅ Trailing stop placed!")
        
        # Monitor for 60 seconds
        print(f"\n📊 Monitoring for 60 seconds...")
        
        for i in range(12):
            ib.sleep(5)
            status = stop_trade.orderStatus.status
            print(f"   [{(i+1)*5}s] Stop: {status}")
            
            if stop_trade.isDone() and status == 'Filled':
                exit_price = stop_trade.orderStatus.avgFillPrice
                pnl = (exit_price - fill_price) * shares
                pnl_pct = ((exit_price - fill_price) / fill_price) * 100
                
                print(f"\n🛑 STOP TRIGGERED!")
                print(f"   Exit:  ${exit_price:.2f}")
                print(f"   P&L:   ${pnl:.2f} ({pnl_pct:+.2f}%)")
                break
        
        # Close if stop didn't trigger
        if stop_trade.orderStatus.status != 'Filled':
            print(f"\n⏹️ Closing position...")
            
            # Cancel stop
            if stop_trade.orderStatus.status in ['PreSubmitted', 'Submitted']:
                ib.cancelOrder(stop_trade.order)
                ib.sleep(1)
            
            # Market order to close
            close_order = MarketOrder('SELL', shares)
            close_order.tif = 'GTC'
            close_order.outsideRth = True
            
            close_trade = ib.placeOrder(contract, close_order)
            
            # Wait for close
            timeout = 15
            elapsed = 0
            while not close_trade.isDone() and elapsed < timeout:
                ib.sleep(1)
                elapsed += 1
            
            if close_trade.orderStatus.status == 'Filled':
                close_price = close_trade.orderStatus.avgFillPrice
                pnl = (close_price - fill_price) * shares
                pnl_pct = ((close_price - fill_price) / fill_price) * 100
                
                print(f"✅ Closed at ${close_price:.2f}")
                print(f"   P&L: ${pnl:.2f} ({pnl_pct:+.2f}%)")
            else:
                print(f"⚠️ Status: {close_trade.orderStatus.status}")
    
    else:
        print(f"\n❌ Not filled: {trade.orderStatus.status}")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    ib.disconnect()
    print("\n✅ Disconnected")