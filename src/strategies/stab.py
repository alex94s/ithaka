# -*- coding: utf-8 -*-
"""
Module Name: stab.py

Description:
The STAB module computes index levels and constituent weights for a statistical arbitrage 
strategy based on short-term mean-reversion signals for a universe of tickers assigned to 
a cluster based on the historical correlation matrix of daily returns.

Notes:
The current backtest includes survivorship bias as the strategy is run on current constituents
of the S&P SmallCap 600 Index. The effect of this bias will be most pronounced in the early years of
the backtest and may be understood as an overstated small-cap premia due to the exclusion of
de-listed companies. Ongoing maintenance of the strategy will not be affected by this bias due
to point-in-time data ingestion which may be used to replicate actual holdings.

Author: elreysausage
Date: 2024-11-18
"""

from datetime import date
import warnings

warnings.filterwarnings("ignore", module="sklearn.cluster._kmeans")

import pandas as pd
from sklearn.cluster import KMeans

from core.strategy import Strategy
import core.utils as ut


class STABStrategy(Strategy):
    """
    The STABStrategy class orchestrates the retrieval of the STAB strategy's signals,
    returns, and constituent weights based on specified parameters.

    Attributes:
        START_DATE: The start date for data retrieval.
        END_DATE: The end date for data retrieval.
        LOOKBACK_WINDOW: The number of days to look back for computing signals.
        SIGNAL_THRESHOLD: The threshold deviation for allowable signals.
        TRADE_LAG: The trade implementation lag.
    """
    START_DATE: str = '2000-01-01'
    END_DATE: str = date.today()
    LOOKBACK_WINDOW: int = 2
    SIGNAL_THRESHOLD: float = 0.0
    TRADE_LAG: int = 1

    def __init__(self, name: str, n_clusters: int, n_sub_strategies):
        """
        Initializes the STABStrategy class with the specified tickers.

        Parameters:
            name: The name of the strategy
            n_clusters: The number of clusters to create for the tickers.
            n_sub_strategies: The number of sub-strategies to consider.
        """
        self.name = name
        self.n_clusters = n_clusters
        self.n_sub_strategies = n_sub_strategies
        self.set_data()
        self.set_params()

    def set_data(self) -> None:
        """
        Sets the data for the STAB strategy.

        Notes:
            The `tickers` attribute is updated by scraping the latest ticker symbols from 
            the S&P SmallCap 600 Wikipedia page. This must be done periodically to ensure the
            strategy is run on the most recent constituents.

            Example:
                To update the NIFTY 500 tickers:
                >>> self.tickers = ut.scrapeTickers(
                        'https://en.wikipedia.org/wiki/List_of_S%26P_600_companies'
                    )
        """
        self.tickers = [
            'AAP', 'AAT', 'ABCB', 'ABG', 'ABM', 'ABR', 'ACA', 'ACIW', 'ACLS', 'ADEA', 
            'ADTN', 'ADUS', 'AEIS', 'AEO', 'AGO', 'AGYS', 'AHCO', 'AHH', 'AIN', 'AIR', 
            'AKR', 'AL', 'ALEX', 'ALG', 'ALGT', 'ALK', 'ALKS', 'ALRM', 'AMBC', 'AMCX', 
            'AMN', 'AMPH', 'AMR', 'AMSF', 'AMWD', 'ANDE', 'ANF', 'ANIP', 'AORT', 'AOSL', 
            'APAM', 'APLE', 'APOG', 'ARCB', 'ARI', 'ARLO', 'AROC', 'ARR', 'ASIX', 
            'ASO', 'ASTE', 'ASTH', 'ATEN', 'ATGE', 'ATI', 'ATNI', 'AUB', 'AVA', 'AVAV', 
            'AVNS', 'AWI', 'AWR', 'AX', 'AXL', 'AZZ', 'B', 'BANC', 'BANF', 'BANR', 'BCC', 
            'BCPC', 'BDN', 'BFH', 'BFS', 'BGC', 'BGS', 'BHE', 'BHLB', 'BJRI', 'BKE', 
            'BKU', 'BL', 'BLMN', 'BMI', 'BOH', 'BOOT', 'BOX', 'BRC', 'BRKL', 
            'BTU', 'BXMT', 'CABO', 'CAKE', 'CAL', 'CALM', 'CA§LX', 'CARG', 'CARS', 
            'CASH', 'CATY', 'CBRL', 'CBU', 'CCOI', 'CCRN', 'CCS', 'CENT', 'CENTA', 
            'CENX', 'CERT', 'CEVA', 'CFFN', 'CHCO', 'CHCT', 'CHEF', 'CHUY', 'CLB', 'CLDT', 
            'CLW', 'CMP', 'CNK', 'CNMD', 'CNS', 'CNXN', 'COHU', 'COLL', 'COOP', 
            'CORT', 'CPF', 'CPK', 'CPRX', 'CRC', 'CRK', 'CRNC', 'CRS', 'CRSR', 'CRVL', 
            'CSGS', 'CSR', 'CTKB', 'CTRE', 'CTS', 'CUBI', 'CVBF', 'CVCO', 'CVGW', 'CVI', 
            'CWEN', 'CWK', 'CWT', 'CXM', 'CXW', 'DAN', 'DBI', 'DCOM', 'DDD', 'DEA', 'DEI', 
            'DFIN', 'DGII', 'DIN', 'DIOD', 'DLX', 'DNOW', 'DOCN', 'DORM', 'DRH', 
            'DV', 'DVAX', 'DXC', 'DXPE', 'DY', 'EAT', 'ECPG', 'EFC', 'EGBN', 'EHAB', 'EIG', 
            'ELME', 'EMBC', 'ENR', 'ENSG', 'ENVA', 'EPAC', 'EPC', 'EPRT', 'ESE', 
            'ETD', 'EVTC', 'EXPI', 'EXTR', 'EYE', 'EZPW', 'FBK', 'FBNC', 'FBP', 'FBRT', 
            'FCF', 'FCPT', 'FDP', 'FELE', 'FFBC', 'FHB', 'FIZZ', 'FL', 'FLGT', 'FN', 
            'FORM', 'FOXF', 'FSS', 'FTDR', 'FTRE', 'FUL', 'FULT', 'FWRD', 'GBX', 'GDEN', 
            'GDOT', 'GEO', 'GES', 'GFF', 'GIII', 'GKOS', 'GMS', 'GNL', 'GOGO', 
            'GPI', 'GPRE', 'GRBK', 'GSHD', 'GTY', 'GVA', 'HAFC', 'HAIN', 'HASI', 
            'HAYW', 'HBI', 'HCC', 'HCI', 'HCSG', 'HFWA', 'HI', 'HIW', 'HLIT', 
            'HLX', 'HMN', 'HNI', 'HOPE', 'HOUS', 'HP', 'HPP', 'HRMY', 'HSII', 'HSTM', 
            'HTH', 'HTLD', 'HUBG', 'HVT', 'HWKN', 'HZO', 'IAC', 'IBP', 'ICHR', 
            'ICUI', 'IDCC', 'IIIN', 'IIPR', 'INDB', 'INN', 'INVA', 'IOSP', 'IPAR', 'IRWD', 
            'ITGR', 'ITRI', 'JACK', 'JBGS', 'JBLU', 'JBSS', 'JJSF', 'JOE', 'JXN', 
            'KALU', 'KAR', 'KELYA', 'KFY', 'KLG', 'KLIC', 'KMT', 'KN', 'KOP', 'KREF', 
            'KSS', 'KTB', 'KW', 'KWR', 'LBRT', 'LCII', 'LESL', 'LGIH', 'LGND', 'LKFN', 
            'LMAT', 'LNC', 'LNN', 'LPG', 'LQDT', 'LRN', 'LTC', 'LUMN', 'LXP', 'LZB', 
            'MAC', 'MARA', 'MATV', 'MATW', 'MATX', 'MBC', 'MC', 'MCRI', 'MCS', 'MCW', 
            'MCY', 'MD', 'MED', 'MEI', 'MERC', 'MGEE', 'MGPI', 'MGY', 'MHO', 'MLAB', 
            'MLKN', 'MLI', 'MMI', 'MMSI', 'MNRO', 'MODG', 'MOV', 'MPW', 'MRCY', 'MRTN', 
            'MSEX', 'MSGS', 'MTH', 'MTRN', 'MTUS', 'MTX', 'MXL', 'MYE', 'MYGN', 'MYRG', 
            'NABL', 'NATL', 'NAVI', 'NBHC', 'NBR', 'NBTB', 'NEO', 'NFBK', 'NGVT', 
            'NMIH', 'NOG', 'NPK', 'NPO', 'NSIT', 'NTCT', 'NUS', 'NVEE', 'NVRI', 
            'NWBI', 'NWL', 'NWN', 'NX', 'NXRT', 'NYMT', 'ODP', 'OFG', 'OGN', 'OI', 'OII', 
            'OMCL', 'OMI', 'OSIS', 'OSUR', 'OTTR', 'OXM', 'PAHC', 'PARR', 'PAYO', 
            'PATK', 'PBH', 'PBI', 'PCRX', 'PDFS', 'PEB', 'PECO', 'PFBC', 'PFS', 
            'PHIN', 'PINC', 'PIPR', 'PJT', 'PLAB', 'PLAY', 'PLMR', 'PLUS', 'PLXS', 'PMT', 
            'POWL', 'PPBI', 'PRA', 'PRAA', 'PRDO', 'PRFT', 'PRG', 'PRGS', 'PRK', 'PRLB', 
            'PRVA', 'PSMT', 'PTEN', 'PUMP', 'PZZA', 'QNST', 'RAMP', 'RC', 'RCUS', 'RDN', 
            'RDNT', 'RES', 'REX', 'REZI', 'RGNX', 'RGP', 'RGR', 'RILY', 'RNST', 'ROCK', 
            'ROG', 'RUN', 'RUSHA', 'RWT', 'RXO', 'SAFE', 'SABR', 'SAFT', 'SAH', 
            'SANM', 'SATS', 'SBCF', 'SBH', 'SBSI', 'SCHL', 'SCL', 'SCSC', 'SCVL', 'SDGR', 
            'SEDG', 'SEE', 'SEM', 'SFBS', 'SFNC', 'SGH', 'SHAK', 'SHEN', 'SHO', 'SHOO', 
            'SIG', 'SITC', 'SITM', 'SJW', 'SKT', 'SKYW', 'SLG', 'SLP', 'SLVM', 'SM', 
            'SMP', 'SMPL', 'SMTC', 'SNCY', 'SNEX', 'SONO', 'SPNT', 'SPSC', 'SPTN', 
            'SPXC', 'SSTK', 'STAA', 'STBA', 'STC', 'STEL', 'STRA', 'SUPN', 'SVC', 'SXC', 
            'SXI', 'SXT', 'TALO', 'TBBK', 'TFIN', 'TGI', 'THRM', 'THRY', 'THS', 'TILE', 
            'TMP', 'TNC', 'TNDM', 'TPH', 'TR', 'TRIP', 'TRMK', 'TRN', 'TRST', 'TRUP', 
            'TTEC', 'TTGT', 'TTMI', 'TWI', 'TWO', 'UCTT', 'UE', 'UFCS', 'UFPT', 'UHT', 
            'UNF', 'UNFI', 'UNIT', 'UPBD', 'URBN', 'USNA', 'USPH', 'UTL', 'UVV', 'VBTX', 
            'VCEL', 'VECO', 'VFC', 'VGR', 'VIAV', 'VICR', 'VIR', 'VRE', 'VREX', 'VRRM', 
            'VRTS', 'VSAT', 'VSCO', 'VSTS', 'VTOL', 'VTLE', 'VYX', 'WABC', 
            'WAFD', 'WD', 'WDFC', 'WGO', 'WLY', 'WNC', 'WOR', 'WRLD', 'WS', 'WSFS', 
            'WSR', 'WWW', 'XHR', 'XNCR', 'XPEL', 'XPER', 'XRX', 'YELP', 'ZEUS'
        ]
        self.price_data = ut.get_prices(
            self.tickers, self.START_DATE, self.END_DATE
        ).ffill()

        self.returns_data = (
            self.price_data.pct_change(fill_method=None)
            .iloc[1:]
            .dropna(axis=1)
        )

    def set_params(self) -> None:
        """
        Sets the parameters for the STAB strategy.

        Notes:
            STAB does not require any additional parameter setting.
        """
        pass

    def get_reversion_signals(
            self, 
            tickers: list, 
            start_date: pd.Timestamp, 
            end_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Computes reversion signals based on the lookback window.

        Parameters:
            tickers: List of tickers to consider.
            start_date: The start date for computing signals.
            end_date: The end date for computing signals.

        Returns:
            signals: A DataFrame containing the reversion signals.
        """
        daily_returns = self.returns_data.loc[start_date:end_date, tickers]
        signals = -(
            daily_returns.rolling(self.LOOKBACK_WINDOW).sum().subtract(
            daily_returns.rolling(self.LOOKBACK_WINDOW).sum().mean(axis=1), axis=0)
        )
        signals[signals.abs()<self.SIGNAL_THRESHOLD] = 0
        signals = signals.divide(signals.abs().sum(axis=1), axis=0)
        return signals

    def get_sub_strategy_returns(
            self, 
            tickers: list, 
            start_date: pd.Timestamp, 
            end_date: pd.Timestamp
    ) -> dict:
        """
        Computes the strategy returns based on the reversion signals.

        Parameters:
            tickers: A list of tickers to consider.
            start_date: The start date for computing returns.
            end_date: The end date for computing returns.

        Returns:
            strategy_returns: A Series containing the strategy
        """
        signals = self.get_reversion_signals(tickers, start_date, end_date)
        strategy_returns = (
            signals
            .shift(self.TRADE_LAG)
            .multiply(self.returns_data.loc[start_date:end_date, tickers])   
            .sum(axis=1)
            .dropna()
        )
        return strategy_returns

    def get_ticker_clusters(self, start_date: pd.Timestamp, end_date: pd.Timestamp) -> dict:
        """
        Clusters stocks based on their daily returns.

        Parameters:
            start_date: The start date for assessing clusters.
            end_date: The end date for assessing clusters.

        Returns:
            A dictionary assigning each eligible stock to a cluster.
        """
        correlation_matrix = self.returns_data.loc[start_date:end_date].corr()
        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42, n_init='auto')
        labels = kmeans.fit_predict(correlation_matrix)
        clusters = {i: [] for i in range(self.n_clusters)}
        for ticker, label in zip(self.returns_data.columns, labels):
            clusters[label].append(ticker)
        return clusters

    def merge_sub_strategy_returns(
            self, 
            clusters: dict, 
            start_date: pd.Timestamp, 
            end_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Merges the sub-strategy returns for each cluster.

        Parameters:
            clusters: A dictionary of stock clusters.
            start_date: The start date for computing returns.
            end_date: The end date for computing returns.

        Returns:
            A DataFrame of strategy results for each cluster.
        """ 
        results = []
        for cluster, cluster_tickers in clusters.items():
            if len(cluster_tickers) > 1:
                try:
                    sub_strategy_returns = self.get_sub_strategy_returns(
                        cluster_tickers, start_date, end_date
                    )
                    instrument_weights = self.get_reversion_signals(
                        cluster_tickers, start_date, end_date
                    )
                    stats = self.get_strategy_statistics(
                        sub_strategy_returns, display_chart=False
                    )
                    stats.update({
                        "Cluster": cluster,
                        "Tickers": cluster_tickers,
                        "Sub-strategy Returns": sub_strategy_returns,
                        "Instrument Weights": instrument_weights
                    })
                    results.append(stats)
                except Exception as e:
                    pass
        return pd.DataFrame(results)
    
    def identify_top_clusters(
            self, 
            clusters: dict, 
            start_date: pd.Timestamp, 
            end_date: pd.Timestamp
    ) -> pd.DataFrame:
        """
        Retrieves the top clusters based on the Sharpe ratio of the sub-strategies.

        Parameters:
            clusters: A dictionary of stock clusters.
            start_date: The start date for computing returns.
            end_date: The end date for computing returns.

        Returns:
            top_clusters: A DataFrame containing the top clusters.
        """
        sub_strategy_returns = self.merge_sub_strategy_returns(
            clusters, start_date, end_date
        )
        top_clusters = (
            sub_strategy_returns.sort_values(by="Sharpe Ratio", ascending=False)
            .head(self.n_sub_strategies)
        )
        return top_clusters
    
    def get_sub_strategy_weights(self, top_clusters: pd.DataFrame) -> list:
        """
        Retrieves the weights for each sub-strategy.

        Parameters:
            top_clusters: A DataFrame containing the top clusters.

        Returns:
            A list of equal weights for each sub-strategy.
        """
        return [1 / len(top_clusters)] * len(top_clusters)

    def get_strategy_returns(self, top_clusters: pd.DataFrame) -> pd.Series:
        """
        Computes the strategy returns based on the top sub-strategies.

        Parameters:
            top_clusters: A DataFrame containing the top clusters.

        Returns:
            strategy_returns: A Series containing the strategy returns.

        """
        strategy_returns = pd.DataFrame(
            {row["Cluster"]: row["Sub-strategy Returns"] 
                for _, row in top_clusters.iterrows()}
        ).mul(self.get_sub_strategy_weights(top_clusters), axis=1).sum(axis=1)
        return strategy_returns
    
    def get_strategy_weights(self, top_clusters: pd.DataFrame) -> pd.DataFrame:
        """
        Retrieves the instrument weights for the top clusters.

        Parameters:
            top_clusters: A DataFrame containing the top clusters.

        Returns:
            strategy_weights: A DataFrame containing the instrument weights.
        """
        strategy_weights = []
        for _, row in top_clusters.iterrows():
            weights = row["Instrument Weights"].reindex(
                columns=self.tickers, fill_value=0)
            strategy_weights.append(weights.div(self.n_sub_strategies))

        strategy_weights = (
            pd.concat(strategy_weights, axis=1)
            .T.groupby(level=0)
            .sum()
            .T
        )
        return strategy_weights

    def get_strategy_output(self) -> dict:
        """
        Retrieves the strategy output for the STAB strategy.

        Returns:
            A dictionary containing the strategy output.
        """
        backtest_start = self.returns_data.index[0]
        backtest_end = self.returns_data.index[-1]
        date = backtest_start + pd.DateOffset(years=5)
        strategy_returns = []
        instrument_weights = []

        while date < backtest_end:
            period_end_date = min(date + pd.DateOffset(years=1), backtest_end)
            clusters = self.get_ticker_clusters(backtest_start, date)
            top_clusters = self.identify_top_clusters(clusters, backtest_start, date)
            cluster_results = self.merge_sub_strategy_returns(
                clusters=top_clusters['Tickers'].to_dict(), 
                start_date=date,
                end_date=period_end_date
            )
            period_returns = self.get_strategy_returns(cluster_results)
            period_weights = self.get_strategy_weights(cluster_results)
            strategy_returns.append(period_returns)
            instrument_weights.append(period_weights)
            date = min(date + pd.DateOffset(years=1), backtest_end)
        
        strategy_returns = pd.concat(strategy_returns)
        instrument_weights = pd.concat(instrument_weights)
        strategy_levels = pd.DataFrame(self.get_strategy_levels(strategy_returns))

        return {
            'Strategy Levels': strategy_levels,
            'Target Weights': instrument_weights,
            'Effective Weights': instrument_weights.shift(self.TRADE_LAG)
        }
