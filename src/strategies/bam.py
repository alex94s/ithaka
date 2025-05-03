# -*- coding: utf-8 -*-
"""
Module Name: bam.py

Description:
The BAM module computes index levels and constituents for a long-only investment strategy
that tactically trades into cash in moments of increased risk using a price momentum filter.
Target/effective constituent weights and historical levels are computed in this module for
further downstream data commits.

Notes:
To create a longer backtest history, Yahoo Finance data has been used prior to 2020. This is
to address the limited historical data available from IBKR, ensuring a more comprehensive
backtest.

Author: elreysausage
Date: 2024-08-10
"""

from datetime import date

import pandas as pd

import core.utils as ut
from core.strategy import Strategy


class BAMStrategy(Strategy):
    """
    The BAMStrategy class orchestrates the retrieval of the BAM strategy's signals,
    returns, and constituent weights based on specified parameters.

    Attributes:
        START_DATE: The start date for data retrieval.
        DATA_SWITCH_DATE: The date at which data source switches from Yahoo to IBKR.
        END_DATE: The end date for data retrieval.
        TRADE_LAG: The number of days to delay trades.
        SIGNAL_DAMPENER: The number of days in which signals are kept stale.
    """
    START_DATE: str = '2000-01-01'
    DATA_SWITCH_DATE: str = '2020-01-01'
    END_DATE: str = date.today()
    TRADE_LAG: int = 2
    SIGNAL_DAMPENER: int = 1

    def __init__(self, name: str, lookback_window: int, rebal_freq: int, signal_update_freq: int):
        """
        Initializes the Strategy class with necessary parameters.

        Parameters:
            name: The name of the strategy.
            lookback_window: The number of days to look back for signal calculations.
            rebal_freq: Rebalacing frequency for the strategy in days.
            signal_update_freq: Signals update frequency in days.
        """
        super().__init__(name)
        self.lookback_window = lookback_window
        self.rebal_freq = rebal_freq
        self.signal_update_freq = signal_update_freq
        self.set_data()
        self.set_params()

    def set_data(self):
        """
        Sets the data for the BAM strategy.

        Notes:
            BAM does not require any external data inputs.
        """
        pass

    def set_params(self) -> None:
        """
        Sets the parameters for the BAM strategy.
        """
        date_range_yahoo = [self.START_DATE, self.DATA_SWITCH_DATE]
        date_range_ibkr = [
            pd.Timestamp(self.DATA_SWITCH_DATE) -
            pd.offsets.BDay(self.lookback_window),
            self.END_DATE
        ]
        asset_tickers = {
            'equity': {'yahoo': ['SPY', 'BIL'], 'ibkr': ['CSPX', 'IB01']},
            'credit': {'yahoo': ['HYG', 'BIL'], 'ibkr': ['IHYA', 'IB01']},
            'rates': {'yahoo': ['TLT', 'BIL'], 'ibkr': ['DTLA', 'IB01']},
            'commodity': {'yahoo': ['GSG', 'BIL'], 'ibkr': ['ICOM', 'IB01']},
            'gold': {'yahoo': ['GLD', 'BIL'], 'ibkr': ['IGLN', 'IB01']},
            'crypto': {'yahoo': ['BTC-USD', 'BIL'], 'ibkr': ['BTC', 'IB01']}
        }
        self.strategy_params_yahoo = {
            asset: [tickers['yahoo'], *date_range_yahoo]
            for asset, tickers in asset_tickers.items()
        }
        self.strategy_params_ibkr = {
            asset: [tickers['ibkr'], *date_range_ibkr]
            for asset, tickers in asset_tickers.items()
        }
        
    def get_signals(self, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        Generates trend signals for a list of tickers based on a specified lookback.

        Arguments:
            tickers: A list of tickers for which to generate signals.
            start_date: The start date for data retrieval.
            end_date: The end date for data retrieval.

        Returns:
            A DataFrame containing trend signals for the specified tickers.
        """
        price_data = ut.get_prices(tickers, start_date, end_date)
        rebal_dates = ut.set_rebal_dates(price_data, self.signal_update_freq)
        trend_score = (
            price_data.rolling(window=self.SIGNAL_DAMPENER).mean()
            .pct_change(self.lookback_window, fill_method=None)
        )
        signals = (
            (trend_score.eq(trend_score.max(1), axis=0))
            .astype(int)[self.lookback_window:]
            .where(rebal_dates == 1)
            .ffill()
        )
        return signals
    
    def get_trend_returns(self, tickers: list[str], start_date: str, end_date: str) -> pd.Series:
        """
        Calculates strategy returns based on trend signals for a given ticker.

        Arguments:
            tickers: A list of tickers for which to calculate strategy returns.
            start_date: The start date for data retrieval.
            end_date: The end date for data retrieval.

        Returns:
            A Series containing the cumulative returns of the strategy.
        """
        signals = self.get_signals(tickers, start_date, end_date)
        strategy_returns = (
            signals.shift(self.TRADE_LAG)
            * ut.get_daily_returns(tickers, start_date, end_date)
        ).sum(axis=1)[self.lookback_window:]
        return ut.get_cum_returns(strategy_returns)
    
    def get_sub_strategy_returns(self, strategy_params: dict[str, list]) -> pd.DataFrame:
        """
        Computes the daily returns for individual sub-strategies.

        Arguments:
            strategy_params: A dictionary with asset classes as keys, 
                and strategy parameters as values.

        Returns:
            strategy_returns: A DataFrame containing daily sub-strategy returns.
        """
        strategy_returns = {}
        for name, params in strategy_params.items():
            strategy_returns[name] = self.get_trend_returns(*params)

        strategy_returns = (
            pd.DataFrame(strategy_returns)
            .ffill()
            .pct_change(fill_method=None)
            .dropna()
        )
        return strategy_returns
    
    def merge_sub_strategy_returns(self) -> pd.Series:
        """
        Computes and merges strategy returns for multiple sets of parameters.

        Returns:
            A Series containing the combined strategy returns.
        """
        combined_strategy_returns = pd.concat([
            self.get_sub_strategy_returns(self.strategy_params_yahoo),
            self.get_sub_strategy_returns(self.strategy_params_ibkr)
        ])[self.SIGNAL_DAMPENER:]
        return combined_strategy_returns
    
    def get_instrument_returns(self) -> pd.DataFrame:
        """
        Retrieves daily returns for the underlying instruments used in the strategy.

        Returns:
            instrument_returns: Daily returns for the specified asset tickers.
        """
        instrument_tickers = {
            v[0][0]: k for k,v in self.strategy_params_yahoo.items() 
            if v and v[0]
        }
        instrument_returns = (
            ut.get_prices(list(instrument_tickers.keys()), self.START_DATE, self.END_DATE)
            .pct_change(fill_method=None)
        )
        instrument_returns.columns = instrument_returns.columns.map(instrument_tickers)
        instrument_returns = instrument_returns[list(self.strategy_params_yahoo.keys())]
        return instrument_returns
    
    def get_strategy_weights(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Computes weights for the underlying instruments held in the strategy.

        Returns:
            A tuple containing:
            - instrument_weights: Instrument weights based on non-delayed signals.
            - effective_weights: Delayed instrument weights for effective weights.
        """
        instrument_weights = {}
        for name, params in self.strategy_params_ibkr.items():
            instrument_weights[name] = self.get_signals(
                *params).multiply(self.sub_strategy_weights[name], axis=0)
        instrument_weights = pd.concat(instrument_weights.values(), axis=1)
        instrument_weights = (
            instrument_weights.T
            .groupby(instrument_weights.columns)
            .sum()
            .T
        )
        effective_weights = instrument_weights.shift(self.TRADE_LAG)
        return instrument_weights, effective_weights
    
    def get_strategy_returns(self) -> pd.Series:
        """
        Computes the daily returns for the combined strategy.

        Returns:
            A Series containing the daily returns for the combined strategy.
        """
        strategy_returns = (
            self.sub_strategy_returns
            * self.sub_strategy_weights.shift()
        ).sum(axis=1)
        return strategy_returns
    
    def get_strategy_output(self) -> dict[str, pd.DataFrame]:
        """
        Generates the strategy output for the BAM strategy.

        Returns:
            A dictionary containing:
                - 'Strategy Levels': The cumulative levels for the combined strategy.
                - 'Sub-strategy Levels': The cumulative levels for all sub-strategies.
                - 'Target Weights': The target weights for each sub-strategy.
                - 'Effective Weights': The effective weights lagged from the target weights.
        """
        self.sub_strategy_returns = self.merge_sub_strategy_returns()
        self.sub_strategy_weights = ut.get_portfolio_weights(
            strategy_returns=self.sub_strategy_returns,
            rebal_freq=self.rebal_freq,
            weighting_scheme='risk_parity',
            training_method='expanding',
            instrument_returns=self.get_instrument_returns().reindex(self.sub_strategy_returns.index)
        )
        strategy_levels = pd.DataFrame(self.get_strategy_levels(self.get_strategy_returns()))
        sub_strategy_levels = pd.DataFrame(self.get_strategy_levels(self.sub_strategy_returns))
        combined_target_weights, combined_effective_weights = self.get_strategy_weights()
        
        return {
            'Strategy Levels': strategy_levels,
            'Sub-strategy Levels': sub_strategy_levels,
            'Target Weights': combined_target_weights,
            'Effective Weights': combined_effective_weights,
        }
