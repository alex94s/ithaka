# -*- coding: utf-8 -*-
"""
Module Name: utils.py

Description:
The Utils module defines utility functions for strategy calculations, 
data retrieval, and performance metrics. It may be used across different
modules to access shared functions.

Author: elreysausage
Date: 2024-08-15
"""

import os
import warnings

warnings.filterwarnings('ignore', module='yfinance')

import ib_insync as ibk
import matplotlib.pyplot as plt
import mysql.connector
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from scipy.optimize import minimize
import yfinance as yf


def print_separator() -> None:
    """
    Prints a horizontal line to separate sections in console output.
    """
    print('-' * 50)


def scrape_tickers(url: str) -> list[str]:
    """
    Scrapes ticker symbols from a Wikipedia page table.

    Arguments:
        url: The URL of the Wikipedia page containing the table of tickers.

    Returns:
        tickers: A list of ticker symbols to be extracted.

    Notes:
        Assumes table has the class "wikitable sortable" with tickers listed in the first column.
        Run function prior to rebalancing to get latest parent constituents.
    """
    response = requests.get(url, timeout=10)
    content = response.text
    soup = BeautifulSoup(content, "html.parser")
    table = soup.find("table", {"class": "wikitable sortable"})
    tickers = []
    rows = table.find_all("tr")[1:]
    for row in rows:
        ticker = row.find_all("td")[0].text.strip()
        tickers.append(ticker)
    return tickers


def get_ticker_info(tickers: list, fields: list) -> dict:
    """
    Retrieves information for the specified tickers.
    
    Arguments:
        tickers: A list of stock tickers.
        fields: A list of fields to retrieve (e.g., 'longName', 'sector', 'industry').
    
    Returns:
        stock_data: A dictionary containing the stock information.
    """
    stock_data = {}
    for ticker in tickers:
        try:
            stock_info = yf.Ticker(ticker).info
            stock_data[ticker] = {field: stock_info.get(field, 'N/A') for field in fields}
        except Exception as e:
            print(f"Failed to retrieve info for {ticker}: {e}")
            stock_data[ticker] = {field: 'N/A' for field in fields}
    return stock_data


def connect_db() -> tuple[
    mysql.connector.MySQLConnection, 
    mysql.connector.cursor.MySQLCursor
]:
    """
    Establishes a connection to the MySQL database and returns the connection and cursor.

    Environment Variables:
        DB_HOST: The hostname or IP address of the database server.
        DB_USER: The username for database authentication.
        DB_PASSWORD: The password for the specified database user.
        DB_NAME: The name of the database to connect to.

    Returns:
        A tuple containing:
        - conn: The database connection.
        - cursor: The database cursor.
    """
    check_env_vars(['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME'])
    conn = mysql.connector.connect(
        host=os.environ.get('DB_HOST'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        database=os.environ.get('DB_NAME')
    )
    cursor = conn.cursor()
    return conn, cursor


def close_db(
        conn: mysql.connector.MySQLConnection,
        cursor: mysql.connector.cursor.MySQLCursor
) -> None:
    """
    Closes the cursor and connection to the MySQL database.

    Arguments:
        conn: The MySQL database connection to be closed.
        cursor: The MySQL cursor to be closed.
    """
    cursor.close()
    conn.close()


def connect_ib(
    host: str = '127.0.0.1',
    port: int = 4001,
    client_id: int = 1,
    max_retries: int = 5
) -> ibk.IB:
    """
    Connects to the Interactive Brokers (IB) API.

    Arguments:
        host: The host address for the IB API (default is '127.0.0.1').
        port: The port number for the IB API (default is 4001).
        clientId: The client ID for the IB connection (default is 1).
        max_retries: The maximum number of retry attempts to connect (default is 5).

    Returns:
        IB: An instance of the IB class connected to the Interactive Brokers API.

    Raises:
        ConnectionError: If the connection fails after the maximum number of retries.
    """
    ib = ibk.IB()
    connected = False
    attempt = 0
    while not connected and attempt < max_retries:
        try:
            ib.connect(host, port, clientId=client_id)
            connected = True
            print(
                f'Connected to IB on attempt {attempt + 1} with clientId {client_id}.')
        except (ConnectionError, TimeoutError) as e:
            print(f"Connection failed: {e}. Retrying...")
            client_id += 1
            attempt += 1
    if not connected:
        raise ConnectionError(
            'Failed to connect to IB after multiple attempts...')
    print_separator()
    return ib


def get_prices(tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Retrieves daily price data from MySQL database.

    Arguments:
        tickers: A list of tickers for data retrieval.
        start_date: The start date for the data retrieval in 'YYYY-MM-DD' format.
        end_date: The end date for the data retrieval in 'YYYY-MM-DD' format.

    Returns:
        price_data: A DataFrame containing the retrieved price data.

    Notes:
        Ensure that all required environment variables are set before running.
    """
    conn, cursor = connect_db()
    query = "SELECT * FROM price_data WHERE symbol IN (%s) AND date BETWEEN %s AND %s"
    params = tickers + [start_date, end_date]
    final_query = query % (','.join(['%s'] * len(tickers)), '%s', '%s')
    try:
        cursor.execute(final_query, params)
        query_result = pd.DataFrame(cursor.fetchall(), columns=[
                                    'symbol', 'date', 'adj_close', 'source'])
        price_data = query_result.pivot_table(
            index='date',
            columns='symbol',
            values='adj_close'
        )
        price_data.index = pd.to_datetime(price_data.index)
    except Exception as e:
        raise RuntimeError("Price data unavailable...") from e
    return price_data


def check_env_vars(required_vars):
    """
    Checks if all required environment variables are set.

    Arguments:
        required_vars: A list of environment variable names to check.

    Raises:
        EnvironmentError: If any required environment variables are missing.
    """
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        raise EnvironmentError(f"Missing environment variables: {', '.join(missing_vars)}. "
                               "Please set them before running this function.")


def get_daily_returns(tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Retrieves daily returns data for specified tickers.

    Arguments:
        tickers: A list of stock tickers to retrieve data for.
        start_date: The start date for the data retrieval in 'YYYY-MM-DD' format.
        end_date: The end date for the data retrieval in 'YYYY-MM-DD' format.

    Returns:
        A DataFrame containing daily percentage returns for each ticker.
    """
    return get_prices(tickers, start_date, end_date).pct_change(fill_method=None)


def get_cum_returns(strategy_returns: pd.Series) -> pd.Series:
    """
    Calculates cumulative returns from daily strategy returns.

    Arguments:
        strategy_returns: A Series containing strategy returns.

    Returns:
        A Series containing cumulative returns.
    """
    return (1 + strategy_returns).cumprod()


def set_rebal_dates(returns: pd.DataFrame, rebal_freq: int) -> pd.Series:
    """
    Generates rebalancing dates based on a specified frequency.

    Arguments:
        returns: A DataFrame containing daily returns data.
        rebal_freq: The frequency at which to rebalance.

    Returns:
        rebal_dates: A Series denoting 1 for a rebalancing date, and 0 otherwise.
    """
    rebal_dates = pd.Series(0, index=returns.index)
    rebal_dates.iloc[::rebal_freq] = 1
    return rebal_dates


def get_portfolio_weights(
    strategy_returns: pd.DataFrame,
    rebal_freq: int,
    weighting_scheme: str,
    training_method: str,
    instrument_returns: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculates portfolio weights based on strategy returns, 
    rebalancing frequency and weighting scheme.

    Arguments:
        strategy_returns: A DataFrame containing daily strategy returns.
        rebal_freq: The frequency at which to rebalance.
        weighting_scheme: The scheme used to determine portfolio weights, 
            (e.g., 'equal', 'risk-parity').
        training_method: The method used for training the weights, 
            (either 'expanding' or 'rolling').
        instrument_returns: A DataFrame containing daily returns of the 
            instruments used in the portfolio.

    Returns:
        weights: A DataFrame containing daily portfolio weights.

    Raises:
        ValueError: If an invalid training method is specified 
            (must be 'expanding' or 'rolling').
    """
    WINDOW_SIZE = 252
    rebal_dates = set_rebal_dates(strategy_returns, rebal_freq)
    weights = pd.DataFrame(columns=strategy_returns.columns,
                           index=strategy_returns.index)
    for i in range(WINDOW_SIZE, len(strategy_returns)):
        if rebal_dates.iloc[i] == 1:
            if training_method == 'expanding':
                weights.iloc[i] = get_rebal_weights(
                    instrument_returns.iloc[:i], weighting_scheme)
            elif training_method == 'rolling':
                weights.iloc[i] = get_rebal_weights(
                    instrument_returns.iloc[i-WINDOW_SIZE:i], weighting_scheme)
            else:
                raise ValueError(
                    "Invalid training method - choose from 'expanding', or 'rolling'...")
        else:
            weights.iloc[i] = weights.iloc[i-1] * (1 + strategy_returns.iloc[i]) / (
                weights.iloc[i-1] * (1 + strategy_returns.iloc[i])).sum()
    weights = weights.infer_objects().fillna(1 / len(weights.columns))
    return weights


def get_portfolio_variance(weights: np.array, returns: pd.DataFrame) -> float:
    """
    Calculates the annualized portfolio variance based on the given weights and returns.

    Arguments:
        weights: An array of portfolio weights.
        returns: A DataFrame containing historical daily returns.

    Returns:
        The annualized standard deviation (volatility) of the portfolio.
    """
    return np.sqrt(np.dot(weights.T, np.dot(returns.cov(), weights))*252)


def get_portfolio_sharpe(weights: np.array, returns: pd.DataFrame) -> float:
    """
    Calculates the Sharpe ratio of the portfolio based on the given weights and returns.

    Arguments:
        weights: An array of portfolio weights.
        returns: A DataFrame containing historical daily returns.

    Returns:
        The negative Sharpe ratio of the portfolio 
            (negative because minimizers are used in optimisation).
    """
    port_ret = (
        np.dot(weights.T, returns.mean(axis=0).to_frame().values) * 252
    )
    port_var = np.sqrt(
        np.dot(weights.T, np.dot(returns.cov(), weights)) * 252
    )
    return float(-1 * (port_ret / port_var))


def get_excess_risk_contributions(weights: np.array, returns: pd.DataFrame) -> float:
    """
    Computes the portfolio excess risk contributions based on given weights and returns.

    Arguments:
        weights: An array of portfolio weights.
        returns: A DataFrame containing historical daily returns.

    Returns:
        The sum of squared deviations from the target risk contributions.
    """
    portfolio_variance = get_portfolio_variance(weights, returns)
    risk_contributions = (
        252 * weights * np.dot(returns.cov(), weights) / portfolio_variance
    )
    target_risk_contributions = np.mean(risk_contributions)
    return np.sum((risk_contributions - target_risk_contributions)**2)


def get_weight_constraint(weights: np.array) -> float:
    """
    Defines a constraint function that ensures the sum of portfolio weights equals 1.

    Arguments:
        weights: An array of portfolio weights.

    Returns:
        The difference between the sum of weights and 1. 
            (A value of 0 means the constraint is satisfied).
    """
    return np.sum(weights) - 1


def get_rebal_weights(returns: pd.DataFrame, weighting_scheme: str) -> list[float]:
    """
    Computes portfolio rebalance weights based on a specified weighting scheme.

    Arguments:
        returns: A DataFrame containing historical instrument returns.
        weighting_scheme: Portfolio weighting scheme
            (Options are 'equal', 'min_variance', 'max_sharpe', or 'risk_parity').

    Returns:
        optimal: A list of target portfolio weights.

    Raises:
        ValueError: If an invalid weighting scheme is specified.
    """
    bounds = [(0, 1) for _ in range(len(returns.columns))]
    init = [1 / len(returns.columns) for _ in range(len(returns.columns))]
    constraint = {'type': 'eq', 'fun': get_weight_constraint}
    if weighting_scheme == 'equal':
        optimal = init
    else:
        if weighting_scheme == 'min_variance':
            objective_function = get_portfolio_variance
        elif weighting_scheme == 'max_sharpe':
            objective_function = get_portfolio_sharpe
        elif weighting_scheme == 'risk_parity':
            objective_function = get_excess_risk_contributions
        else:
            raise ValueError(
                "Invalid weighting scheme - choose from 'equal', "
                "'min_variance', 'max_sharpe', or 'risk_parity'...")
        optimal = list(
            minimize(
                fun=objective_function,
                x0=init,
                args=(returns,),
                bounds=bounds,
                constraints=constraint,
                method='SLSQP'
            )['x']
        )
    return optimal


def get_perf_stats(strategy_returns: pd.DataFrame, display_chart: bool) -> dict[str, float]:
    """
    Calculates performance statistics for a given strategy.

    Arguments:
        strategy_returns: DataFrame containing daily strategy returns.
        display_chart: A boolean indicating whether to plot the performance chart.

    Returns:
        dict[str, float]: A dictionary with performance metrics.
    """
    cum_returns = get_cum_returns(strategy_returns).squeeze()
    ann_return = (
        cum_returns.iloc[-1] ** 
        (365 / (cum_returns.index[-1] - cum_returns.index[0]).days)
    ) - 1
    ann_vol = np.sqrt(252) * strategy_returns.std()
    drawdown = cum_returns / cum_returns.cummax() - 1
    max_drawdown = -drawdown.min()
    sharpe_ratio = ann_return / ann_vol
    calmar_ratio = ann_return / max_drawdown if max_drawdown != 0 else 0
    if display_chart:
        plot_perf(strategy_returns, benchmark_ticker=None)
    return {
        'Annualized Return': f'{round(ann_return * 100, 2)}%',
        'Annualized Volatility': f'{round(ann_vol * 100, 2)}%',
        'Sharpe Ratio': f'{round(sharpe_ratio, 2)}',
        'Maximum Drawdown': f'{round(max_drawdown * 100, 2)}%',
        'Calmar Ratio': f'{round(calmar_ratio, 2)}',
    }


def get_yearly_returns(strategy_returns: pd.DataFrame) -> pd.Series:
    """
    Calculates the calendar year returns for a given strategy.

    Arguments:
        strategy_returns: A DataFrame containing daily strategy returns.

    Returns:
        A Series containing calendar year returns.
    """
    return (
        get_cum_returns(strategy_returns)
        .resample('Y')
        .apply(lambda x: x[-1])
        .pct_change()
        .dropna()
        .apply(lambda x: f"{x * 100:.2f}%")
    )


def get_trade_count(instrument_weights: pd.DataFrame) -> float:
    """
    Calculates the annual trade count based on significant changes in instrument weights.

    Arguments:
        instrument_weights: A DataFrame containing daily instrument weights.

    Returns:
        ann_trade_count: The annualized number of trades.
    """
    EPSILON = 0.01
    total_trade_count = (
        abs(instrument_weights.diff() > EPSILON)
        .astype(int)
        .sum()
    )
    ann_trade_count = (
        total_trade_count * 365
        / (instrument_weights.index[-1] - instrument_weights.index[0]).days
    )
    return ann_trade_count


def plot_perf(strategy_returns: pd.DataFrame, benchmark_ticker: str = None) -> None:
    """
    Plots the cumulative returns of a strategy compared to a benchmark.

    Arguments:
        strategy_returns: A DataFrame containing daily strategy returns.
        benchmark_ticker: The ticker symbol of the benchmark to compare against.
    """
    plt.figure(figsize=(12, 6))
    plt.plot(np.log(get_cum_returns(strategy_returns)), label="Strategy")
    if benchmark_ticker:
        benchmark_prices = get_prices(
            [benchmark_ticker], 
            strategy_returns.index[0], 
            strategy_returns.index[-1]
        )
        benchmark_returns = get_cum_returns(benchmark_prices.pct_change())
        plt.plot(np.log(benchmark_returns), label=f"Benchmark ({benchmark_ticker})")
    plt.xlabel('Date')
    plt.ylabel('Cumulative Returns')
    plt.legend(loc="upper left")
    plt.show()
