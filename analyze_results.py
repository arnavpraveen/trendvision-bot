import pandas as pd
from datetime import datetime

def analyze_trades():
    """Analyze trading bot performance"""
    
    try:
        df = pd.read_csv('trade_log.csv')
    except FileNotFoundError:
        print("❌ No trade_log.csv found. Run the bot first!")
        return
    
    if len(df) == 0:
        print("📊 No trades yet. Waiting for alerts...")
        return
    
    print("\n" + "="*60)
    print("📊 TRADING BOT PERFORMANCE ANALYSIS")
    print("="*60)
    
    # Split buys and sells
    buys = df[df['action'] == 'BUY']
    sells = df[df['action'] == 'SELL']
    
    print(f"\n📈 TRADE SUMMARY:")
    print(f"   Total Buys: {len(buys)}")
    print(f"   Total Sells: {len(sells)}")
    print(f"   Open Positions: {len(buys) - len(sells)}")
    
    # Calculate P&L for closed positions
    total_pnl = 0
    wins = 0
    losses = 0
    
    print(f"\n💰 CLOSED POSITIONS:")
    print("-" * 60)
    
    for ticker in buys['ticker'].unique():
        buy_trades = buys[buys['ticker'] == ticker]
        sell_trades = sells[sells['ticker'] == ticker]
        
        # Match each buy with corresponding sell
        for idx, buy in buy_trades.iterrows():
            matching_sells = sell_trades[sell_trades.index > idx]
            
            if len(matching_sells) > 0:
                sell = matching_sells.iloc[0]
                
                shares = buy['shares']
                entry = buy['price']
                exit_price = sell['price']
                
                pnl = (exit_price - entry) * shares
                pnl_pct = ((exit_price - entry) / entry) * 100
                
                total_pnl += pnl
                
                if pnl > 0:
                    wins += 1
                    emoji = "✅"
                else:
                    losses += 1
                    emoji = "❌"
                
                print(f"{emoji} {ticker:6} | Entry: ${entry:7.4f} | Exit: ${exit_price:7.4f} | P&L: ${pnl:7.2f} ({pnl_pct:+6.2f}%)")
    
    # Summary statistics
    if wins + losses > 0:
        print("-" * 60)
        print(f"\n📊 STATISTICS:")
        print(f"   Win Rate: {(wins/(wins+losses)*100):.1f}% ({wins}W / {losses}L)")
        print(f"   Total P&L: ${total_pnl:,.2f}")
        print(f"   Avg P&L per trade: ${total_pnl/(wins+losses):,.2f}")
        
        if wins > 0:
            avg_win = buys[buys['ticker'].isin(sells['ticker'])].apply(
                lambda x: (sells[sells['ticker']==x['ticker']].iloc[0]['price'] - x['price']) * x['shares'] if len(sells[sells['ticker']==x['ticker']]) > 0 else 0, 
                axis=1
            ).mean()
            if avg_win > 0:
                print(f"   Avg Win: ${avg_win:.2f}")
        
        # Best and worst trades
        if len(buys) > 0 and len(sells) > 0:
            print(f"\n🏆 BEST TRADE:")
            best_pnl = -999999
            best_ticker = ""
            
            for ticker in buys['ticker'].unique():
                buy = buys[buys['ticker'] == ticker].iloc[-1]
                sell_rows = sells[sells['ticker'] == ticker]
                if len(sell_rows) > 0:
                    sell = sell_rows.iloc[-1]
                    pnl = (sell['price'] - buy['price']) * buy['shares']
                    if pnl > best_pnl:
                        best_pnl = pnl
                        best_ticker = ticker
                        best_entry = buy['price']
                        best_exit = sell['price']
            
            if best_ticker:
                best_pct = ((best_exit - best_entry) / best_entry) * 100
                print(f"   {best_ticker}: ${best_pnl:.2f} ({best_pct:+.2f}%)")
    
    # Open positions
    open_positions = set(buys['ticker']) - set(sells['ticker'])
    if open_positions:
        print(f"\n🔓 OPEN POSITIONS:")
        for ticker in open_positions:
            buy = buys[buys['ticker'] == ticker].iloc[-1]
            print(f"   {ticker}: {buy['shares']} shares @ ${buy['price']:.4f}")
    
    # Recent trades
    print(f"\n📅 RECENT TRADES (last 10):")
    print("-" * 60)
    recent = df.tail(10).sort_values('timestamp', ascending=False)
    for idx, trade in recent.iterrows():
        time = pd.to_datetime(trade['timestamp']).strftime('%m/%d %H:%M')
        action = trade['action']
        ticker = trade['ticker']
        shares = trade['shares']
        price = trade['price']
        
        if action == 'BUY':
            emoji = "📈"
        else:
            emoji = "📉"
        
        print(f"{emoji} {time} | {action:4} | {ticker:6} | {shares:3} shares @ ${price:7.4f}")
    
    print("\n" + "="*60 + "\n")

if __name__ == "__main__":
    analyze_trades()