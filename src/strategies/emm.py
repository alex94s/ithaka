# -*- coding: utf-8 -*-
"""
Module Name: emm.py

Description:
The EMM module computes index levels and constituents for a long-only momentum strategy
targeting Emerging Markets. It includes a risk regime indicator to tactically 
short equity index futures in moments of heightened volatility. Target/effective constituent 
weights and historical levels are computed in this module for further downstream data commits.

Notes:
The current backtest includes survivorship bias as the strategy is run on current constituents
of the NIFTY 500 index. The effect of this bias will be most pronounced in the early years of
the backtest and may be understood as an overstated small-cap premia due to the exclusion of
de-listed companies. Ongoing maintenance of the strategy will not be affected by this bias due
to point-in-time data ingestion which may be used to replicate actual holdings.

Author: elreysausage
Date: 2024-08-10
"""

from datetime import date

import pandas as pd

import core.utils as ut
from core.strategy import Strategy


class EMMStrategy(Strategy):
    """
    The EMMStrategy class orchestrates the retrieval of the EMM strategy's signals,
    returns, and constituent weights based on specified parameters.

    Attributes:
        START_DATE: The start date for data retrieval.
        END_DATE: The end date for data retrieval.
        EQUITY_HEDGE_TICKER: The ticker symbol for the equity hedge.
        FX_TICKER: The ticker symbol for the FX rate.
        FX_HEDGE_TICKER: The threshold for triggering FX re-hedging.
        EQUITY_HEDGE_THRESHOLD: The threshold for triggering equity re-hedging.
        SIGNAL_DAMPENER: The number of days in which signals are kept stale.
        FX_HEDGE_THRESHOLD: The threshold for the FX hedge.
    """
    START_DATE: str = '2010-01-01'
    END_DATE: str = date.today()
    EQUITY_HEDGE_TICKER: str = 'ES=F'
    FX_TICKER: str = 'INR=X'
    FX_HEDGE_TICKER: str = 'SIR=F'
    EQUITY_HEDGE_THRESHOLD: float = 0.2
    FX_HEDGE_THRESHOLD: float = 0.2
    SIGNAL_DAMPENER: int = 5

    def __init__(self, name: str, lookback_window: int, n_stocks: int, rebal_freq: int):
        """
        Initializes the Strategy class with necessary parameters.

        Parameters:
            name: The name of the strategy.
            lookback_window: The number of days to look back for signal calculations.
            n_stocks: The number of stocks to hold in the strategy.
            rebal_freq: Rebalacing frequency for the strategy in days.
        """
        super().__init__(name)
        self.lookback_window = lookback_window
        self.n_stocks = n_stocks
        self.rebal_freq = rebal_freq
        self.set_data()
        self.set_params()

    def set_data(self):
        """
        Sets the data for the EMM strategy.

        Notes:
            The `tickers` attribute is updated by scraping the latest ticker symbols from 
            the NIFTY 500 Wikipedia page. This must be done periodically to ensure the
            strategy is run on the most recent constituents.

            Example:
                To update the NIFTY 500 tickers:
                >>> self.tickers = ut.scrapeTickers('https://en.wikipedia.org/wiki/NIFTY_500')
        """
        self.tickers = [
            '360ONE.NS', '3MINDIA.NS', 'ABB.NS', 'ACC.NS', 'AIAENG.NS',
            'APLAPOLLO.NS', 'AUBANK.NS', 'AARTIIND.NS', 'AAVAS.NS',
            'ABBOTINDIA.NS', 'ACE.NS', 'ADANIENSOL.NS', 'ADANIENT.NS',
            'ADANIGREEN.NS', 'ADANIPORTS.NS', 'ADANIPOWER.NS', 'ATGL.NS',
            'AWL.NS', 'ABCAPITAL.NS', 'ABFRL.NS', 'AEGISLOG.NS', 'AETHER.NS',
            'AFFLE.NS', 'AJANTPHARM.NS', 'APLLTD.NS', 'ALKEM.NS',
            'ALKYLAMINE.NS', 'ALLCARGO.NS', 'ALOKINDS.NS', 'ARE&M.NS',
            'AMBER.NS', 'AMBUJACEM.NS', 'ANANDRATHI.NS', 'ANGELONE.NS',
            'ANURAS.NS', 'APARINDS.NS', 'APOLLOHOSP.NS', 'APOLLOTYRE.NS',
            'APTUS.NS', 'ACI.NS', 'ASAHIINDIA.NS', 'ASHOKLEY.NS',
            'ASIANPAINT.NS', 'ASTERDM.NS', 'ASTRAZEN.NS', 'ASTRAL.NS',
            'ATUL.NS', 'AUROPHARMA.NS', 'AVANTIFEED.NS', 'DMART.NS',
            'AXISBANK.NS', 'BEML.NS', 'BLS.NS', 'BSE.NS',
            'BAJAJ-AUTO.NS', 'BAJFINANCE.NS', 'BAJAJFINSV.NS', 'BAJAJHLDNG.NS',
            'BALAMINES.NS', 'BALKRISIND.NS', 'BALRAMCHIN.NS', 'BANDHANBNK.NS',
            'BANKBARODA.NS', 'BANKINDIA.NS', 'MAHABANK.NS', 'BATAINDIA.NS',
            'BAYERCROP.NS', 'BERGEPAINT.NS', 'BDL.NS', 'BEL.NS',
            'BHARATFORG.NS', 'BHEL.NS', 'BPCL.NS', 'BHARTIARTL.NS',
            'BIKAJI.NS', 'BIOCON.NS', 'BIRLACORPN.NS', 'BSOFT.NS',
            'BLUEDART.NS', 'BLUESTARCO.NS', 'BBTC.NS', 'BORORENEW.NS',
            'BOSCHLTD.NS', 'BRIGADE.NS', 'BRITANNIA.NS', 'MAPMYINDIA.NS',
            'CCL.NS', 'CESC.NS', 'CGPOWER.NS', 'CIEINDIA.NS',
            'CRISIL.NS', 'CSBBANK.NS', 'CAMPUS.NS', 'CANFINHOME.NS',
            'CANBK.NS', 'CAPLIPOINT.NS', 'CGCL.NS', 'CARBORUNIV.NS',
            'CASTROLIND.NS', 'CEATLTD.NS', 'CELLO.NS', 'CENTRALBK.NS',
            'CDSL.NS', 'CENTURYPLY.NS', 'CENTURYTEX.NS', 'CERA.NS',
            'CHALET.NS', 'CHAMBLFERT.NS', 'CHEMPLASTS.NS', 'CHENNPETRO.NS',
            'CHOLAHLDNG.NS', 'CHOLAFIN.NS', 'CIPLA.NS', 'CUB.NS',
            'CLEAN.NS', 'COALINDIA.NS', 'COCHINSHIP.NS', 'COFORGE.NS',
            'COLPAL.NS', 'CAMS.NS', 'CONCORDBIO.NS', 'CONCOR.NS',
            'COROMANDEL.NS', 'CRAFTSMAN.NS', 'CREDITACC.NS', 'CROMPTON.NS',
            'CUMMINSIND.NS', 'CYIENT.NS', 'DCMSHRIRAM.NS', 'DLF.NS',
            'DOMS.NS', 'DABUR.NS', 'DALBHARAT.NS', 'DATAPATTNS.NS',
            'DEEPAKFERT.NS', 'DEEPAKNTR.NS', 'DELHIVERY.NS', 'DEVYANI.NS',
            'DIVISLAB.NS', 'DIXON.NS', 'LALPATHLAB.NS', 'DRREDDY.NS',
            'EIDPARRY.NS', 'EIHOTEL.NS', 'EPL.NS', 'EASEMYTRIP.NS',
            'ECLERX.NS', 'EICHERMOT.NS', 'ELECON.NS', 'ELGIEQUIP.NS',
            'EMAMILTD.NS', 'ENDURANCE.NS', 'ENGINERSIN.NS', 'EQUITASBNK.NS',
            'ERIS.NS', 'ESCORTS.NS', 'EXIDEIND.NS', 'FDC.NS',
            'NYKAA.NS', 'FEDERALBNK.NS', 'FACT.NS', 'FINEORG.NS',
            'FINCABLES.NS', 'FINPIPE.NS', 'FSL.NS', 'FIVESTAR.NS',
            'FORTIS.NS', 'GAIL.NS', 'GMMPFAUDLR.NS',
            'GRSE.NS', 'GICRE.NS', 'GILLETTE.NS', 'GLAND.NS',
            'GLAXO.NS', 'GLENMARK.NS', 'MEDANTA.NS',
            'GPIL.NS', 'GODFRYPHLP.NS', 'GODREJCP.NS', 'GODREJIND.NS',
            'GODREJPROP.NS', 'GRANULES.NS', 'GRAPHITE.NS', 'GRASIM.NS',
            'GESHIP.NS', 'GRINDWELL.NS', 'GAEL.NS', 'FLUOROCHEM.NS',
            'GUJGASLTD.NS', 'GMDCLTD.NS', 'GNFC.NS', 'GPPL.NS',
            'GSFC.NS', 'GSPL.NS', 'HEG.NS',
            'HCLTECH.NS', 'HDFCAMC.NS', 'HDFCBANK.NS',
            'HFCL.NS', 'HAPPSTMNDS.NS', 'HAPPYFORGE.NS', 'HAVELLS.NS',
            'HEROMOTOCO.NS', 'HSCL.NS', 'HINDALCO.NS', 'HAL.NS',
            'HINDCOPPER.NS', 'HINDPETRO.NS', 'HINDUNILVR.NS', 'HINDZINC.NS',
            'POWERINDIA.NS', 'HOMEFIRST.NS', 'HONASA.NS', 'HONAUT.NS',
            'HUDCO.NS', 'ICICIBANK.NS', 'ICICIGI.NS', 'ICICIPRULI.NS',
            'ISEC.NS', 'IDBI.NS', 'IDFCFIRSTB.NS', 'IDFC.NS',
            'IIFL.NS', 'IRB.NS', 'IRCON.NS', 'ITC.NS',
            'ITI.NS', 'INDIACEM.NS', 'IBULHSGFIN.NS', 'INDIAMART.NS',
            'INDIANB.NS', 'IEX.NS', 'INDHOTEL.NS', 'IOC.NS',
            'IOB.NS', 'IRCTC.NS', 'IRFC.NS', 'INDIGOPNTS.NS',
            'IGL.NS', 'INDUSTOWER.NS', 'INDUSINDBK.NS', 'NAUKRI.NS',
            'INFY.NS', 'INOXWIND.NS', 'INTELLECT.NS', 'INDIGO.NS',
            'IPCALAB.NS', 'JBCHEPHARM.NS', 'JKCEMENT.NS', 'JBMA.NS',
            'JKLAKSHMI.NS', 'JKPAPER.NS', 'JMFINANCIL.NS', 'JSWENERGY.NS',
            'JSWINFRA.NS', 'JSWSTEEL.NS', 'JAIBALAJI.NS', 'J&KBANK.NS',
            'JINDALSAW.NS', 'JSL.NS', 'JINDALSTEL.NS', 'JIOFIN.NS',
            'JUBLFOOD.NS', 'JUBLINGREA.NS', 'JUBLPHARMA.NS', 'JWL.NS',
            'JUSTDIAL.NS', 'JYOTHYLAB.NS', 'KPRMILL.NS', 'KEI.NS',
            'KNRCON.NS', 'KPITTECH.NS', 'KRBL.NS', 'KSB.NS',
            'KAJARIACER.NS', 'KPIL.NS', 'KALYANKJIL.NS', 'KANSAINER.NS',
            'KARURVYSYA.NS', 'KAYNES.NS', 'KEC.NS', 'KFINTECH.NS',
            'KOTAKBANK.NS', 'KIMS.NS', 'LTF.NS', 'LTTS.NS',
            'LICHSGFIN.NS', 'LTIM.NS', 'LT.NS', 'LATENTVIEW.NS',
            'LAURUSLABS.NS', 'LXCHEM.NS', 'LEMONTREE.NS', 'LICI.NS',
            'LINDEINDIA.NS', 'LLOYDSME.NS', 'LUPIN.NS', 'MMTC.NS',
            'MRF.NS', 'MTARTECH.NS', 'LODHA.NS', 'MGL.NS',
            'MAHSEAMLES.NS', 'M&MFIN.NS', 'M&M.NS', 'MHRIL.NS',
            'MAHLIFE.NS', 'MANAPPURAM.NS', 'MRPL.NS', 'MANKIND.NS',
            'MARICO.NS', 'MARUTI.NS', 'MASTEK.NS', 'MFSL.NS',
            'MAXHEALTH.NS', 'MAZDOCK.NS', 'MEDPLUS.NS', 'METROBRAND.NS',
            'METROPOLIS.NS', 'MINDACORP.NS', 'MSUMI.NS', 'MOTILALOFS.NS',
            'MPHASIS.NS', 'MCX.NS', 'MUTHOOTFIN.NS', 'NATCOPHARM.NS',
            'NCC.NS', 'NHPC.NS', 'NLCINDIA.NS', 'NMDC.NS', 'NSLNISP.NS', 
            'NTPC.NS', 'NH.NS', 'NATIONALUM.NS', 'NAVINFLUOR.NS', 
            'NESTLEIND.NS', 'NETWORK18.NS', 'NAM-INDIA.NS', 'NUVAMA.NS', 
            'NUVOCO.NS', 'OBEROIRLTY.NS', 'ONGC.NS', 'OIL.NS', 'OLECTRA.NS', 
            'PAYTM.NS', 'OFSS.NS', 'POLICYBZR.NS', 'PCBL.NS', 'PIIND.NS',
            'PNBHOUSING.NS', 'PNCINFRA.NS', 'PVRINOX.NS', 'PAGEIND.NS',
            'PATANJALI.NS', 'PERSISTENT.NS', 'PETRONET.NS', 'PHOENIXLTD.NS',
            'PIDILITIND.NS', 'PEL.NS', 'PPLPHARMA.NS', 'POLYMED.NS',
            'POLYCAB.NS', 'POONAWALLA.NS', 'PFC.NS', 'POWERGRID.NS',
            'PRAJIND.NS', 'PRESTIGE.NS', 'PRINCEPIPE.NS', 'PRSMJOHNSN.NS',
            'PGHH.NS', 'PNB.NS', 'QUESS.NS', 'RRKABEL.NS',
            'RBLBANK.NS', 'RECLTD.NS', 'RHIM.NS', 'RITES.NS',
            'RADICO.NS', 'RVNL.NS', 'RAILTEL.NS', 'RAINBOW.NS',
            'RAJESHEXPO.NS', 'RKFORGE.NS', 'RCF.NS', 'RATNAMANI.NS',
            'RTNINDIA.NS', 'RAYMOND.NS', 'REDINGTON.NS', 'RELIANCE.NS',
            'RBA.NS', 'ROUTE.NS', 'SBFC.NS', 'SBICARD.NS',
            'SJVN.NS', 'SKFINDIA.NS', 'SRF.NS', 'SAFARI.NS',
            'MOTHERSON.NS', 'SANOFI.NS', 'SAPPHIRE.NS', 'SAREGAMA.NS',
            'SCHAEFFLER.NS', 'SCHNEIDER.NS', 'SHREECEM.NS', 'RENUKA.NS',
            'SHRIRAMFIN.NS', 'SHYAMMETL.NS', 'SIEMENS.NS', 'SIGNATURE.NS',
            'SOBHA.NS', 'SOLARINDS.NS', 'SONACOMS.NS', 'SONATSOFTW.NS',
            'STARHEALTH.NS', 'SBIN.NS', 'SAIL.NS', 'SWSOLAR.NS',
            'STLTECH.NS', 'SUMICHEM.NS', 'SPARC.NS', 'SUNPHARMA.NS',
            'SUNTV.NS', 'SUNDARMFIN.NS', 'SUNDRMFAST.NS', 'SUNTECK.NS',
            'SUPREMEIND.NS', 'SUVENPHAR.NS', 'SUZLON.NS', 'SWANENERGY.NS',
            'SYNGENE.NS', 'SYRMA.NS', 'TV18BRDCST.NS', 'TVSMOTOR.NS',
            'TVSSCS.NS', 'TMB.NS', 'TANLA.NS', 'TATACHEM.NS',
            'TATACOMM.NS', 'TCS.NS', 'TATACONSUM.NS', 'TATAELXSI.NS',
            'TATAINVEST.NS', 'TATAMTRDVR.NS', 'TATAMOTORS.NS', 'TATAPOWER.NS',
            'TATASTEEL.NS', 'TATATECH.NS', 'TTML.NS', 'TECHM.NS',
            'TEJASNET.NS', 'NIACL.NS', 'RAMCOCEM.NS', 'THERMAX.NS',
            'TIMKEN.NS', 'TITAGARH.NS', 'TITAN.NS', 'TORNTPHARM.NS',
            'TORNTPOWER.NS', 'TRENT.NS', 'TRIDENT.NS', 'TRIVENI.NS',
            'TRITURBINE.NS', 'TIINDIA.NS', 'UCOBANK.NS', 'UNOMINDA.NS',
            'UPL.NS', 'UTIAMC.NS', 'UJJIVANSFB.NS', 'ULTRACEMCO.NS',
            'UNIONBANK.NS', 'UBL.NS', 'UNITDSPR.NS', 'USHAMART.NS',
            'VGUARD.NS', 'VIPIND.NS', 'VAIBHAVGBL.NS', 'VTL.NS',
            'VARROC.NS', 'VBL.NS', 'MANYAVAR.NS', 'VEDL.NS',
            'VIJAYA.NS', 'IDEA.NS', 'VOLTAS.NS', 'WELCORP.NS',
            'WELSPUNLIV.NS', 'WESTLIFE.NS', 'WHIRLPOOL.NS', 'WIPRO.NS',
            'YESBANK.NS', 'ZFCVINDIA.NS', 'ZEEL.NS', 'ZENSARTECH.NS',
            'ZOMATO.NS', 'ZYDUSLIFE.NS'
        ]

    def set_params(self) -> None:
        """
        Sets the parameters for the EMM strategy.

        Notes:
            EMM does not require any additional parameter setting.
        """
        pass
    
    def get_momentum_score(self, price_data) -> pd.DataFrame:
        """
        Calculates the momentum score for each asset based on historical price data.

        Parameters:
            price_data: A DataFrame containing historical price data.

        Returns:
            momentum_score: Momentum scores, excluding the initial lookback period.
        """
        self.momentum_score = (
            price_data.pct_change(self.lookback_window, fill_method=None) /
            price_data.pct_change(fill_method=None).rolling(self.lookback_window).std()
        )[self.lookback_window:]
        return self.momentum_score

    def get_target_weights(self) -> pd.DataFrame:
        """
        Computes target weights for a strategy based on momentum signals and rebal dates.

        Returns:
            target_weights: A DataFrame with target weights for each stock.
        """
        target_weights = pd.DataFrame(
            index=self.eq_momentum_signals.index, columns=self.eq_momentum_signals.columns)
        selected_stocks = pd.DataFrame(
            index=self.eq_momentum_signals.index, columns=range(0, self.n_stocks))
        for i in self.eq_momentum_signals.index:
            if self.rebal_dates.loc[i] == 1:
                selected_stocks.loc[i] = (
                    self.eq_momentum_signals.loc[i].nlargest(self.n_stocks).index
                )
                current_selection = selected_stocks.loc[i]
            else:
                selected_stocks.loc[i] = current_selection
            target_weights.loc[i, selected_stocks.loc[i]] = 1 / self.n_stocks
        return target_weights.infer_objects().fillna(0)

    def get_strategy_weights(self) -> pd.DataFrame:
        """
        Calculates intra-rebalance strategy weights by applying daily returns to target weights.

        Returns:
            effective_weights: A DataFrame containing the drifted target weights.

        Notes:
            - On rebalancing dates, the weights are set to the values from `targetWeights`.
            - On non-rebalancing dates, weights are updated based on daily returns, and rebased.
        """
        effective_weights = pd.DataFrame(
            columns=self.target_weights.columns, index=self.target_weights.index)
        effective_weights.loc[self.rebal_dates == 1] = self.target_weights
        for i in range(1, len(effective_weights)):
            if effective_weights.iloc[i].isnull().all():
                try:
                    effective_weights.iloc[i] = (
                        (effective_weights.iloc[i-1] * (1 + self.daily_returns.iloc[i])) /
                        (effective_weights.iloc[i-1] * (1 + self.daily_returns.iloc[i])).sum()
                    )
                except (ZeroDivisionError, ValueError):
                    effective_weights.iloc[i] = effective_weights.iloc[i-1]
        effective_weights = effective_weights.infer_objects().fillna(0)
        return effective_weights
    
    def get_equity_returns(self) -> None:
        """
        Computes equity returns based on momentum scores, rebalancing frequency, 
        and stock selection.
        """
        self.price_data = ut.get_prices(self.tickers, self.START_DATE, self.END_DATE)
        self.eq_momentum_signals = self.get_momentum_score(self.price_data).shift()
        self.rebal_dates = ut.set_rebal_dates(self.eq_momentum_signals, self.rebal_freq)
        self.daily_returns = self.price_data.pct_change(fill_method=None)[self.lookback_window:]
        self.target_weights = self.get_target_weights()
        self.effective_weights = self.get_strategy_weights()
        self.equity_returns = (self.daily_returns*self.effective_weights.shift()).sum(axis=1)

    def get_hedge_returns(self) -> pd.Series:
        """
        Computes the returns of a hedging strategy by selectively shorting equity index futures.

        Returns:
            Daily returns of the equity hedge strategy.
        """
        equity_hedge_data = (
            ut.get_prices([self.EQUITY_HEDGE_TICKER], self.START_DATE, self.END_DATE)
            .reindex(self.equity_returns.index)
            .ffill()
            .squeeze()
        )
        equity_hedge_returns = equity_hedge_data.pct_change(fill_method=None)

        risk_indicator = (
            self.get_momentum_score(equity_hedge_data)
            .rolling(window=self.SIGNAL_DAMPENER).mean() > 0
        ).astype(int)

        self.equity_hedge_ratio = (
            self.equity_returns.rolling(self.lookback_window).cov(equity_hedge_returns) /
            equity_hedge_returns.rolling(self.lookback_window).var()
        ).reindex(risk_indicator.index)
        self.equity_hedge_ratio[self.equity_hedge_ratio < self.EQUITY_HEDGE_THRESHOLD] = 0

        self.drifted_equity_hedge_ratio = pd.Series(
            index=risk_indicator.index,
            dtype=float
        )
        for i in range(2, len(risk_indicator)):
            if risk_indicator.iloc[i-1] == 0:
                if risk_indicator.iloc[i-2] != 0:
                    self.drifted_equity_hedge_ratio.iloc[i] = self.equity_hedge_ratio.iloc[i-1]
                else:
                    self.drifted_equity_hedge_ratio.iloc[i] = (
                        self.drifted_equity_hedge_ratio.iloc[i-1] *
                        (1 - equity_hedge_returns.iloc[i]) /
                        (1 + self.equity_returns.iloc[i + self.lookback_window])
                    )
            else:
                self.drifted_equity_hedge_ratio.iloc[i] = 0

        equity_hedge_returns = (
            self.drifted_equity_hedge_ratio.shift()
            * equity_hedge_returns
        ).dropna()

        self.drifted_equity_hedge_ratio.name = self.EQUITY_HEDGE_TICKER
        self.equity_hedge_ratio = (
            self.equity_hedge_ratio * (1 - risk_indicator)
        ).rename(self.EQUITY_HEDGE_TICKER)

        return equity_hedge_returns
    
    def get_strategy_returns(self) -> pd.Series:
        """
        Computes the returns of a dollar-denominated equity strategy with hedging adjustments.

        Returns:
            A Series with the strategy returns.
        """
        self.get_equity_returns()
        equity_hedge_returns = self.get_hedge_returns()
        fx_returns = (
            ut.get_prices([self.FX_TICKER], self.START_DATE, self.END_DATE)
            .reindex(self.equity_returns.index)
            .ffill()
            .squeeze()
            .pct_change(fill_method=None)
        )
        fx_hedge_returns = (
            ut.get_prices([self.FX_HEDGE_TICKER], self.START_DATE, self.END_DATE)
            .reindex(self.equity_returns.index)
            .ffill()
            .squeeze()
            .pct_change(fill_method=None)
        )
        self.fx_hedge_ratio = pd.Series(
            1, index=self.equity_returns.index, name=self.FX_HEDGE_TICKER)
        self.drifted_fx_hedge_ratio = pd.Series(
            1, index=self.equity_returns.index, name=self.FX_HEDGE_TICKER)

        for i in range(1, len(self.drifted_fx_hedge_ratio)):
            fx_hedge_deviation = abs(self.drifted_fx_hedge_ratio.iloc[i-1] - self.fx_hedge_ratio.iloc[i-1])
            if  fx_hedge_deviation > self.FX_HEDGE_THRESHOLD:
                self.drifted_fx_hedge_ratio.iloc[i] = self.fx_hedge_ratio.iloc[i]
            else:
                self.drifted_fx_hedge_ratio.iloc[i] = (self.drifted_fx_hedge_ratio.iloc[i-1] *
                                            (1 - fx_hedge_returns.iloc[i]) /
                                            (1 + self.equity_returns.iloc[i] - fx_returns.iloc[i]))
                self.drifted_fx_hedge_ratio = self.drifted_fx_hedge_ratio.fillna(1)
        return (
            (1 + self.equity_returns)
            * (1 - equity_hedge_returns)
            * (1 - fx_returns)
            * (1 - self.drifted_fx_hedge_ratio.shift() * fx_hedge_returns)
            - 1
        ).dropna()
    
    def get_strategy_output(self) -> dict[str, pd.DataFrame]:
        """
        Generates the strategy output for the EMM strategy.

        Returns:
            A dictionary containing:
                - 'Strategy Levels': The cumulative levels for the combined strategy.
                - 'Target Weights': The target weights for each sub-strategy.
                - 'Effective Weights': The effective weights lagged from the target weights.
        """
        strategy_returns = self.get_strategy_returns()
        self.effective_weights = pd.concat(
            [self.effective_weights,
             self.drifted_equity_hedge_ratio.mul(-1),
             self.drifted_fx_hedge_ratio.mul(-1)],
            axis=1
        ).fillna(0)[strategy_returns.index[0]:]

        self.target_weights = pd.concat(
            [self.target_weights,
            -self.equity_hedge_ratio,
            -self.fx_hedge_ratio],
            axis=1
        ).fillna(0)[strategy_returns.index[0]:]

        strategy_levels = pd.DataFrame(
            self.get_strategy_levels(strategy_returns)
        ).rename_axis(index=1)

        return {
            'Strategy Levels': strategy_levels,
            'Target Weights': self.target_weights,
            'Effective Weights': self.effective_weights,
        }
