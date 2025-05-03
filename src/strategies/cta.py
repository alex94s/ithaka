# -*- coding: utf-8 -*-
"""
Module Name: cta.py

Description:
The CTA module computes index levels and constituents for a suite of futures trading
strategies based on momentum, autocorrelation and seasonality effects in key asset classes.
Target/effective constituent weights and historical levels are computed in this module for
further downstream data commits.

Notes:
To create a longer backtest history, Yahoo Finance data has been used prior to 2023. This is
to address the limited historical data available from IBKR, ensuring a more comprehensive
backtest.

Author: elreysausage
Date: 2024-08-10
"""

from datetime import date

import numpy as np
import pandas as pd

from core.strategy import Strategy
import core.utils as ut


class CTAStrategy(Strategy):
    """
    The CTAStrategy class orchestrates the retrieval of the CTA strategy's signals,
    returns, and constituent weights based on specified parameters.

    Attributes:
        START_DATE: The start date for data retrieval.
        END_DATE: The end date for data retrieval.
        DATA_SWITCH_DATE: The date at which data source switches from Yahoo to IBKR.
    """
    START_DATE: str = '2002-01-01'
    END_DATE: str = date.today()
    DATA_SWITCH_DATE: str = '2022-06-30'

    def __init__(self, name: str, lookback_window: int, rebal_freq: int, target_vol: float):
        """
        Initializes the Strategy class with necessary parameters.

        Parameters:
            name: The name of the strategy.
            lookback_window: The number of days to look back for signal calculations.
            rebal_freq: Rebalacing frequency for the strategy in days.
            target_vol: The target volatility for the strategy.
        """
        super().__init__(name)
        self.lookback_window = lookback_window
        self.rebal_freq = rebal_freq
        self.target_vol = target_vol
        self.set_data()
        self.set_params()

    def set_data(self):
        """
        Sets the data for the BAM strategy.

        Notes:
            CTA does not require any external data inputs.
        """
        pass

    def set_params(self) -> None:
        """
        Sets the parameters for the CTA strategy.
        """
        date_range_yahoo = [self.START_DATE, self.DATA_SWITCH_DATE]
        date_range_ibkr = [
            pd.Timestamp(self.DATA_SWITCH_DATE) -
            pd.offsets.BDay(self.lookback_window),
            self.END_DATE
        ]
        self.strategy_params_yahoo = {
            'equity': [self.get_autocorrelation_returns(['ES=F'], *date_range_yahoo)],
            'rates': [self.get_trend_returns(['ZQ=F'], *date_range_yahoo, True)],
            'commodity': [self.get_commodity_seasonality_returns(
                    [['CT=F', 'ZS=F', 'ZC=F'], [12, 1, 2], *date_range_yahoo],
                    [['HO=F', 'NG=F'], [8, 9, 10], *date_range_yahoo])],
            'fx': [self.get_trend_returns(['DX=F'], *date_range_yahoo, False)],
            'volatility': [self.get_insurance_returns(['VIXM'], ['^VIX'], *date_range_yahoo)],
            'housing': [self.get_trend_returns(['CUS'], *date_range_yahoo, False)],
            'dividends': [self.get_trend_returns(['SDA=F'], *date_range_yahoo, False)],
        }
        self.strategy_params_ibkr = {
            'equity': [self.get_autocorrelation_returns(['MES'], *date_range_ibkr)],
            'rates': [self.get_trend_returns(['ZQ'], *date_range_ibkr, True)],
            'commodity': [self.get_commodity_seasonality_returns(
                    [['TT', 'ZS', 'ZC'], [12, 1, 2], *date_range_ibkr],
                    [['HO'], [8, 9, 10], *date_range_ibkr])],   # Removed NG temporarily
            'fx': [self.get_trend_returns(['DX=F'], *date_range_ibkr, False)],
            'volatility': [self.get_insurance_returns(['VIXM'], ['VIX'], *date_range_ibkr)],
            'housing': [self.get_trend_returns(['CUS'], *date_range_ibkr, False)],
            'dividends': [self.get_trend_returns(['SDA=F'], *date_range_ibkr, False)],
        }

    def get_autocorrelation_returns(
            self,
            tickers: list[str], 
            start_date: str, 
            end_date: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generates returns for an equity index autocorrelation strategy.

        Arguments:
            tickers: A list of tickers to analyze.
            start_date: The start date for the data retrieval in 'YYYY-MM-DD' format.
            end_date: The end date for the data retrieval in 'YYYY-MM-DD' format.

        Returns:
            A tuple containing:
            - strategy_returns: Non-leveraged returns of an autocorrelation strategy.
            - signals: A DataFrame of strategy signals.
        """
        WINDOW_SIZE = 252
        SIGNAL_THRESHOLD = 0.1
        price_data = ut.get_prices(tickers, start_date, end_date)
        daily_returns = price_data.pct_change(fill_method=None).squeeze()
        autocorr = (
            daily_returns.rolling(WINDOW_SIZE)
            .apply(lambda x: x.autocorr(), raw=False)
            .squeeze()
        )
        signals = pd.Series(0, index=price_data.index)
        for i, autocorr_value in enumerate(autocorr):
            if autocorr_value > SIGNAL_THRESHOLD:
                signals.iloc[i] = (
                    (daily_returns.iloc[i] > 0).astype(int)
                    - (daily_returns.iloc[i] < 0).astype(int)
                )
            elif autocorr_value < -SIGNAL_THRESHOLD:
                signals.iloc[i] = (
                    (daily_returns.iloc[i] < 0).astype(int) 
                    - (daily_returns.iloc[i] > 0).astype(int)
                )
            else:
                signals.iloc[i] = 0
        strategy_returns = (
            (signals.shift() * daily_returns)[WINDOW_SIZE:].squeeze()
        )
        return strategy_returns, signals

    def get_trend_returns(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
        enable_shorts: bool
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generates returns for a trend-following strategy.

        Arguments:
            tickers: A list of tickers to analyze.
            start_date: The start date for the data retrieval in 'YYYY-MM-DD' format.
            end_date: The end date for the data retrieval in 'YYYY-MM-DD' format.
            enable_shorts: A boolean flag that determines whether short selling is enabled. 

        Returns:
            A tuple containing:
            - strategy_returns: Non-leveraged returns of a trend-following strategy.
            - signals: A DataFrame of strategy signals.
        """
        price_data = ut.get_prices(tickers, start_date, end_date)
        daily_returns = ut.get_daily_returns(tickers, start_date, end_date)
        if enable_shorts:
            signals = (
                (price_data.pct_change(self.lookback_window, fill_method=None) > 0).astype(int) - 
                (price_data.pct_change(self.lookback_window, fill_method=None) < 0).astype(int)
            )
        else:
            signals = (
                price_data.pct_change(self.lookback_window, fill_method=None) > 0
            ).astype(int)
        strategy_returns = (
            (signals.shift() * daily_returns)
            .mean(axis=1)[self.lookback_window:]
            .squeeze()
        )
        return strategy_returns, signals

    def get_seasonality_returns(
            self,
            tickers: list[str],
            buy_months: list[int],
            start_date: str,
            end_date: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generates returns for a commodities seasonality strategy.

        Arguments:
            tickers: A list of tickers to analyze.
            buy_months: A list of months during which underlying instruments may be purchased.
            start_date: The start date for the data retrieval in 'YYYY-MM-DD' format.
            end_date: The end date for the data retrieval in 'YYYY-MM-DD' format.

        Returns:
            A tuple containing:
            - strategy_returns: Non-leveraged returns of a seasonality strategy.
            - signals: A DataFrame of strategy signals.
        """
        price_data = ut.get_prices(tickers, start_date, end_date)
        daily_returns = ut.get_daily_returns(tickers, start_date, end_date)
        regime_indicator = (
            price_data.pct_change(self.lookback_window, fill_method=None) > 0
        ).astype(int)
        signals = pd.Series(0, index=price_data.index)
        for year in price_data.index.year.unique():
            for month in buy_months:
                signals.loc[(price_data.index.year == year) &
                            (price_data.index.month == month)] = 1
        signals = regime_indicator.multiply(signals, axis=0)
        strategy_returns = (signals.shift()*daily_returns).mean(axis=1)
        return strategy_returns, signals

    def get_commodity_seasonality_returns(
            self,
            ags_params: list,
            energy_params: list
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generates aggregate returns for commodity ags and energy seasonality strategies.

        Arguments:
            ags_params: A list containing parameters for ags seasonality strategy.
            energy_params: A list containing parameters for energy seasonality strategy.

        Returns:
            A tuple containing:
            - strategy_returns: Aggregated non-leveraged returns of seasonality strategies.
            - signals: A DataFrame of aggregated strategy signals.
        """
        strategy_returns = (
            self.get_seasonality_returns(*ags_params)[0] +
            self.get_seasonality_returns(*energy_params)[0]
        )[self.lookback_window:].dropna()

        ags_signals = self.get_seasonality_returns(*ags_params)[1]
        energy_signals = self.get_seasonality_returns(*energy_params)[1]
        
        signals = pd.merge(
            ags_signals, energy_signals,
            left_index=True, right_index=True,
            how='inner'
        )
        return strategy_returns, signals

    def get_insurance_returns(
            self,
            instrument_ticker: list[str],
            vix_ticker: list[str],
            start_date: str,
            end_date: str
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Generates returns for a volatility insurance strategy.

        Arguments:
            instrument_ticker: The ticker for the traded instrument.
            vix_ticker: The ticker for the VIX Index.
            start_date: The start date for the data retrieval in 'YYYY-MM-DD' format.
            end_date: The end date for the data retrieval in 'YYYY-MM-DD' format.

        Returns:
            A tuple containing:
            - strategy_returns: Non-leveraged returns of an insurance strategy.
            - signals: DataFrame of strategy signals.
        """
        daily_returns = ut.get_daily_returns(instrument_ticker, start_date, end_date)
        vix = ut.get_prices(vix_ticker, start_date, end_date)
        rolling_quantiles = (
            vix.rolling(window=self.lookback_window, center=False)
            .quantile(0.99)
        )
        signals = (vix > rolling_quantiles).astype(int)
        signals = signals.rename(columns={vix_ticker[0]: instrument_ticker[0]})
        strategy_returns = (
            (signals.shift() * daily_returns)[self.lookback_window:]
            .squeeze()
            .dropna()
        )
        return strategy_returns, signals

    def get_leverage_factor(
            self,
            strategy_returns: pd.DataFrame,
            rebal_freq: int,
            training_method: str
    ) -> pd.Series:
        """
        Calculates the leverage factor for a strategy based on its returns and target volatility.

        Arguments:
            strategy_returns: DataFrame of strategy returns.
            rebal_freq: Frequency of rebalancing.
            training_method: Method used for calculating volatility,
                either 'expanding' or 'rolling'.

        Returns:
            Series containing the leverage factor for each date.

        Notes:
            The leverage factor is determined by comparing the target volatility with the rolling,
            or expanding volatility of the strategy returns. The function applies a cap on the 
            leverage factor to avoid excessive leverage.
        """
        WINDOW_SIZE = 252
        LEVERAGE_CAP = 20
        rebal_dates = ut.set_rebal_dates(strategy_returns, rebal_freq)
        leverage_factor = pd.Series(index=strategy_returns.index, dtype=float)
        for i in range(WINDOW_SIZE, len(strategy_returns)):
            if rebal_dates.iloc[i] == 1:
                if not strategy_returns[strategy_returns != 0].iloc[:i].dropna().empty:
                    if training_method == 'expanding':
                        leverage_factor.iloc[i] = min(
                            LEVERAGE_CAP,
                            round(
                                self.target_vol / (
                                    strategy_returns[strategy_returns != 0]
                                    .iloc[:i]
                                    .std() * np.sqrt(252)
                                )
                            )
                        )
                    elif training_method == 'rolling':
                        leverage_factor.iloc[i] = min(
                            LEVERAGE_CAP,
                            round(
                                self.target_vol / (
                                    strategy_returns[strategy_returns != 0]
                                    .iloc[i - WINDOW_SIZE:i]
                                    .std() * np.sqrt(252)
                                )
                            )
                        )
                    else:
                        raise ValueError(
                            "Invalid training method - choose from 'expanding' or 'rolling'...")
                else:
                    leverage_factor.iloc[i] = 1
        leverage_factor = leverage_factor.ffill().replace(0, 1).fillna(1)
        return leverage_factor
    
    def get_leveraged_returns(
            self,
            strategy_returns: tuple[pd.DataFrame, pd.DataFrame]
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Calculates leveraged returns based on strategy returns and target volatility.

        Arguments:
            strategy_returns: A tuple containing:
            - A DataFrame containing strategy returns.
            - A DataFrame containing signals associated with the strategy returns.

        Returns:
            A tuple containing:
            - leveraged_returns: Leveraged returns of strategy returns, and leverage factor.
            - signals: A DataFrame of strategy signals.
        """
        leveraged_returns = self.get_leverage_factor(
            strategy_returns[0], self.rebal_freq, 'expanding') * strategy_returns[0]
        signals = strategy_returns[1]
        return leveraged_returns, signals
    
    def merge_leveraged_returns(self, strategy_params: dict[str, list]) -> pd.DataFrame:
        """
        Merges leveraged returns for all strategies defined in the strategy parameters.

        Arguments:
            strategy_params: A dictionary with strategy names as keys,
                and parameters required by `getLeveragedReturns` function as values.

        Returns:
            strategy_returns: Merged daily leveraged returns for all sub-strategies.
        """
        strategy_returns = {}
        for name, params in strategy_params.items():
            strategy_returns[name] = self.get_leveraged_returns(*params)[0]
        strategy_returns = pd.DataFrame(strategy_returns).fillna(0)
        return strategy_returns

    def get_sub_strategy_returns(self) -> pd.DataFrame:
        """
        Generates merged daily sub-strategy returns using both Yahoo and IBKR data.

        Returns:
            A DataFrame of returns for all sub-strategies.
        """
        combined_strategy_returns = pd.concat([
            self.merge_leveraged_returns(self.strategy_params_yahoo),
            self.merge_leveraged_returns(self.strategy_params_ibkr)
        ])
        return combined_strategy_returns

    def get_strategy_weights(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Calculates constituent weights based on both delayed and target weights.

        Returns:
            A tuple containing:
            - instrument_weights: DataFrame with the merged instrument weights.
            - effective_weights: DataFrame with the lagged instrument weights.
        """
        instrument_weights = {}
        for name, params in self.strategy_params_ibkr.items():
            signals = self.get_leveraged_returns(*params)[1].reindex(self.sub_strategy_weights.index)
            instrument_weights[name] = signals.multiply(self.sub_strategy_weights[name], axis=0)
            
        instrument_weights = pd.concat(instrument_weights.values(), axis=1)
        instrument_weights = (
            instrument_weights.T
            .groupby(instrument_weights.columns)
            .sum()
            .T[pd.Timestamp(self.DATA_SWITCH_DATE) + pd.offsets.BDay(self.lookback_window):]
        )
        effective_weights = instrument_weights.shift()
        return instrument_weights, effective_weights
    
    def get_strategy_returns(self) -> pd.DataFrame:
        """
        Generates daily returns for the CTA strategy.

        Returns:
            A DataFrame of daily returns for the CTA strategy.
        """
        strategy_returns = (
            self.sub_strategy_returns 
            * self.sub_strategy_weights.shift()
        ).sum(axis=1)
        return strategy_returns.dropna()
    
    def get_strategy_output(self) -> dict[str, pd.DataFrame]:
        """
        Generates the strategy output for the CTA strategy.

        Returns:
            A dictionary containing:
                - 'Strategy Levels': The cumulative levels for the combined strategy.
                - 'Sub-strategy Levels': The cumulative levels for all sub-strategies.
                - 'Target Weights': The target weights for each sub-strategy.
                - 'Effective Weights': The effective weights lagged from the target weights.
        """
        self.sub_strategy_returns = self.get_sub_strategy_returns()
        self.sub_strategy_weights = ut.get_portfolio_weights(
            strategy_returns=self.sub_strategy_returns,
            rebal_freq=self.rebal_freq,
            weighting_scheme='equal',
            training_method='expanding',
            instrument_returns=self.sub_strategy_returns
        )
        strategy_levels = pd.DataFrame(self.get_strategy_levels(self.get_strategy_returns()))
        sub_strategy_levels = pd.DataFrame(self.get_strategy_levels(self.sub_strategy_returns)).fillna(1)
        combined_target_weights, combined_effective_weights = (
            self.get_strategy_weights()
        )
        return {
            'Strategy Levels': strategy_levels,
            'Sub-strategy Levels': sub_strategy_levels,
            'Target Weights': combined_target_weights,
            'Effective Weights': combined_effective_weights,
        }
