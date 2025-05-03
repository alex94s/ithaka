# -*- coding: utf-8 -*-
"""
Module Name: tracker.py

Description:
The Tracker module compares current portfolio holdings against target positions 
and generates trade orders to be executed via the IBKR (Interactive Brokers) API. 
This module helps in rebalancing the portfolio to align with the target allocations.

Author: elreysausage
Date: 2024-05-30
"""

import ib_insync as ibk
import pandas as pd

from core.factory import DataManager
import core.utils as ut


def get_nav(cursor: ut.mysql.connector.cursor.MySQLCursor) -> float:
    """
    Retrieves the Net Asset Value (NAV) of the IBKR account for the latest available date.

    Arguments:
        cursor: The MySQL cursor for executing queries.

    Returns:
        nav: The IBKR account NAV.
    """
    query = (
        "SELECT adj_close FROM price_data "
        "WHERE date = (SELECT MAX(date) FROM price_data WHERE symbol = 'ITK') "
        "AND symbol = 'ITK'"
    )
    cursor.execute(query)
    nav = float(pd.DataFrame(cursor.fetchall())[0])
    return nav


def get_last_prices(cursor: ut.mysql.connector.cursor.MySQLCursor) -> pd.DataFrame:
    """
    Retrieves the last available prices for all symbols from the price data table.

    Arguments:
        cursor: The MySQL cursor for executing queries.

    Returns:
        last_prices: A DataFrame containing symbols and their last available prices.
    """
    query = (
        "SELECT symbol, adj_close FROM price_data "
        "WHERE (symbol, date) IN "
        "(SELECT symbol, MAX(date) FROM price_data GROUP BY symbol)"
    )
    cursor.execute(query)
    last_prices = pd.DataFrame(cursor.fetchall(), columns=[
        'symbol', 'adj_close'])
    return last_prices


def get_positions(
        cursor: ut.mysql.connector.cursor.MySQLCursor,
        weight_type: str
) -> pd.DataFrame:
    """
    Retrieves the current positions for a given weight type from the strategy weights table.

    Arguments:
        cursor: The MySQL cursor for executing queries.
        weight_type: The type of weight to retrieve ('portfolio' or 'target').

    Returns:
        positions: A DataFrame containing the symbols and their corresponding positions.
    """
    query = (
        f"SELECT symbol, {weight_type}_weight FROM strategy_weights "
        "WHERE date = (SELECT MAX(date) FROM strategy_weights) "
        "AND strategy != 'EMM'"
    )
    cursor.execute(query)
    weights = pd.DataFrame(
         cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    weights.set_index("symbol", inplace=True)
    positions = get_nav(cursor) * weights
    positions.columns = ["position"]
    return positions


def get_required_trades(
        epsilon: float, 
        run_mode: str, 
        run_automated_trades: bool = False
) -> pd.DataFrame:
    """
    Computes trades required to adjust the current portfolio to match target positions.

    Arguments:
        epsilon: The threshold for determining if a trade is required.
        run_mode: The mode in which the application is running ('live' or 'test').
        run_automated_trades: A boolean flag to indicate if trades should be executed

    Returns:
        required_trades: Details of required trades(symbol, action, notional, quantity).
    """
    if run_mode == 'live':
        ibk.util.startLoop()
    else:
        print('Loading app...')
    conn, cursor = ut.connect_db()
    dm = DataManager(run_mode)
    dm.run_updates()
    current_positions = get_positions(cursor, 'portfolio')
    target_positions = get_positions(cursor, 'target')
    nav = get_nav(cursor)
    last_prices = get_last_prices(cursor)
    contract_multipliers = set_contract_multipliers()
    delta = (target_positions-current_positions).round(2)
    required_trades = (
        delta[delta / nav > epsilon]
        .dropna()
        .reset_index()
    )
    required_trades.columns = ['symbol', 'notional']
    required_trades = (
        pd.merge(required_trades, last_prices, on='symbol')
        .merge(contract_multipliers, on='symbol', how='left')
        .fillna({'multiplier': 1})
    )
    required_trades['action'] = required_trades['notional'].apply(
        lambda x: 'BUY' if x > 0 else 'SELL'
    )
    required_trades['notional'] = required_trades['notional'].abs()
    required_trades['quantity'] = (
        required_trades['notional'] / 
        (required_trades['adj_close'] * required_trades['multiplier'])
    )
    ut.close_db(conn, cursor)
    if run_automated_trades and run_mode == 'live':
        execute_trades(dm, required_trades)
        required_trades = None
    return required_trades


def set_contract_multipliers() -> pd.DataFrame:
    """
    Sets the contract multipliers for different symbols.
    
    Returns:
        contract_multipliers: A Series containing the contract multipliers for different symbols.
    
    Notes:
        Contract multipliers correct as of Aug-24.
    """
    contract_multipliers = pd.Series({
        'MES': 5,
        'ZQ': 4167,
        'TT': 50000,
        'ZS': 50,
        'ZC': 50,
        'DX=F': 1000,
        'VXM': 100,
        'CUS=F': 250,
        'SDA=F': 250
    })
    return (
        contract_multipliers
        .reset_index(name='multiplier')
        .rename(columns={'index': 'symbol'})
    )


def execute_trades(data_manager: DataManager, required_trades: pd.DataFrame) -> None:
    """
    Executes the required trades to adjust the current portfolio to match target positions.

    Arguments:
        data_manager: An instance of the DataManager class.
        required_trades: Details of required trades(symbol, action, notional, quantity).
    """
    for idx, row in required_trades.iterrows():
        symbol = row['symbol']
        action = row['action']
        contract = data_manager.ticker_map[symbol]
        quantity = round(row['quantity'], 4)
        # order = ibk.MarketOrder(action, quantity)
        # trade = ibk.placeOrder(contract, order)
        # print(trade.log)
        # print(trade.orderStatus.status)
    print('Trades have been placed...')
    ut.print_separator()
