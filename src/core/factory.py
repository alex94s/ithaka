# -*- coding: utf-8 -*
"""
Module Name: factory.py

Description:
The Factory module provides functionality for uploading price data, strategy levels, 
and constituent weights to a MySQL database. The Data Manager class interacts with 
the IBKR and Yahoo Finance APIs to download price data and utilises the strategy 
modules (BAM, CTA, EMM, NEWT, STAB, FAR) to compute historical/live strategy levels 
and weights to upload to the database.

Author: elreysausage
Date: 2024-05-30
"""

import sys
import warnings
from datetime import date

warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', module='yfinance')

import ib_insync as ibk
import pandas as pd
import yfinance as yf

import core.utils as ut
from strategies.bam import BAMStrategy
from strategies.cta import CTAStrategy
from strategies.emm import EMMStrategy
from strategies.newt import NEWTStrategy
from strategies.stab import STABStrategy
from strategies.far import FARStrategy


class DataManager(object):
    """
    Manages data connections, retrieval, and commits for productionised strategies.

    Attributes:
        run_mode: The mode in which the script is run ('live' or 'test').
        conn: A connection object to the MySQL database.
        cursor: A cursor object for executing SQL queries.
        insert_query: A SQL query for inserting data into the database.
        ib: An IBKR connection object for data retrieval.

    Notes:
        The run_mode attribute determines whether the DataManager should download
        and commit data to the database ('live') or skip the commit process ('test').
    """
    def __init__(self, run_mode: str):
        """
        Initializes the DataManager class with necessary parameters.

        Arguments:
            run_mode: The mode in which the script is run ('live' or 'test').
        """
        self.run_mode = run_mode
        self.conn, self.cursor = ut.connect_db()
        self.insert_query = (
            "INSERT INTO price_data (date, adj_close, symbol, source) "
            "VALUES (%s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "date = date, adj_close = adj_close, symbol = symbol, source = source"
        )
        self.load_tickers()
        if self.run_mode == 'live':
            self.ib = ut.connect_ib()
            try:
                self.load_strategies()
            except (ImportError, RuntimeError, AttributeError, KeyError) as e:
                self.create_tables()
                self.commit_ticker_prices()
                print(f"Module import failed: {e}. Please re-run the script...")
                ut.print_separator()
                sys.exit(1)

    def load_tickers(self) -> None:
        """
        Loads required tickers for data retrieval from IBKR and Yahoo Finance.
        """
        self.ticker_map = {
            'CSPX': ibk.Stock('CSPX', 'LSEETF', 'USD'),
            'IB01': ibk.Stock('IB01', 'LSEETF', 'USD'),
            'IHYA': ibk.Stock('IHYA', 'LSEETF', 'USD'),
            'DTLA': ibk.Stock('DTLA', 'LSEETF', 'USD'),
            'ICOM': ibk.Stock('ICOM', 'LSEETF', 'USD'),
            'IGLN': ibk.Stock('IGLN', 'LSEETF', 'USD'),
            'BTC': ibk.Crypto('BTC', 'PAXOS', 'USD'),
            'VIX': ibk.Index('VIX', 'CBOE'),
            'MES': ibk.ContFuture('MES', 'CME'),
            'ZQ': ibk.ContFuture('ZQ', 'CBOT'),
            'TT': ibk.ContFuture('TT', 'NYMEX'),
            'ZS': ibk.ContFuture('ZS', 'CBOT'),
            'ZC': ibk.ContFuture('ZC', 'CBOT'),
            'HO': ibk.ContFuture('HO', 'NYMEX'),
            # 'NG': ibk.ContFuture('NG', 'NYMEX'),
            # 'VXM': ibk.ContFuture('VXM', 'CFE'),
            'CUS': ibk.ContFuture('CUS', 'CME'),
        }
        self.ticker_list = list(self.ticker_map.keys())
        self.yahoo_tickers = [
            'SPY', 'HYG', 'TLT', 'GSG', 'GLD', 'BTC-USD', 'BIL', 'ES=F', 'ZQ=F', 'CT=F',
            'ZS=F', 'ZC=F', 'HO=F', 'NG=F', 'DX=F', 'VIXM', '^VIX', 'SDA=F', 'FALN'
        ]
        self.yahoo_tickers += [
            '360ONE.NS', '3MINDIA.NS', 'ABB.NS', 'ACC.NS', 'AIAENG.NS', 'APLAPOLLO.NS',
            'AUBANK.NS', 'AARTIIND.NS', 'AAVAS.NS', 'ABBOTINDIA.NS', 'ACE.NS',
            'ADANIENSOL.NS', 'ADANIENT.NS', 'ADANIGREEN.NS', 'ADANIPORTS.NS',
            'ADANIPOWER.NS', 'ATGL.NS', 'AWL.NS', 'ABCAPITAL.NS', 'ABFRL.NS',
            'AEGISLOG.NS', 'AETHER.NS', 'AFFLE.NS', 'AJANTPHARM.NS', 'APLLTD.NS',
            'ALKEM.NS', 'ALKYLAMINE.NS', 'ALLCARGO.NS', 'ALOKINDS.NS', 'ARE&M.NS',
            'AMBER.NS', 'AMBUJACEM.NS', 'ANANDRATHI.NS', 'ANGELONE.NS', 'ANURAS.NS',
            'APARINDS.NS', 'APOLLOHOSP.NS', 'APOLLOTYRE.NS', 'APTUS.NS', 'ACI.NS',
            'ASAHIINDIA.NS', 'ASHOKLEY.NS', 'ASIANPAINT.NS', 'ASTERDM.NS', 'ASTRAZEN.NS',
            'ASTRAL.NS', 'ATUL.NS', 'AUROPHARMA.NS', 'AVANTIFEED.NS',
            'AXISBANK.NS', 'BEML.NS', 'BLS.NS', 'BSE.NS', 'BAJAJ-AUTO.NS', 'BAJFINANCE.NS',
            'BAJAJFINSV.NS', 'BAJAJHLDNG.NS', 'BALAMINES.NS', 'BALKRISIND.NS',
            'BALRAMCHIN.NS', 'BANDHANBNK.NS', 'BANKBARODA.NS', 'BANKINDIA.NS',
            'MAHABANK.NS', 'BATAINDIA.NS', 'BAYERCROP.NS', 'BERGEPAINT.NS', 'BDL.NS',
            'BEL.NS', 'BHARATFORG.NS', 'BHEL.NS', 'BPCL.NS', 'BHARTIARTL.NS', 'BIKAJI.NS',
            'BIOCON.NS', 'BIRLACORPN.NS', 'BSOFT.NS', 'BLUEDART.NS', 'BLUESTARCO.NS',
            'BBTC.NS', 'BORORENEW.NS', 'BOSCHLTD.NS', 'BRIGADE.NS', 'BRITANNIA.NS',
            'MAPMYINDIA.NS', 'CCL.NS', 'CESC.NS', 'CGPOWER.NS', 'CIEINDIA.NS', 'CRISIL.NS',
            'CSBBANK.NS', 'CAMPUS.NS', 'CANFINHOME.NS', 'CANBK.NS', 'CAPLIPOINT.NS',
            'CGCL.NS', 'CARBORUNIV.NS', 'CASTROLIND.NS', 'CEATLTD.NS', 'CELLO.NS',
            'CENTRALBK.NS', 'CDSL.NS', 'CENTURYPLY.NS', 'CERA.NS',
            'CHALET.NS', 'CHAMBLFERT.NS', 'CHEMPLASTS.NS', 'CHENNPETRO.NS',
            'CHOLAHLDNG.NS', 'CHOLAFIN.NS', 'CIPLA.NS', 'CUB.NS', 'CLEAN.NS',
            'COALINDIA.NS', 'COCHINSHIP.NS', 'COFORGE.NS', 'COLPAL.NS', 'CAMS.NS',
            'CONCORDBIO.NS', 'CONCOR.NS', 'COROMANDEL.NS', 'CRAFTSMAN.NS',
            'CREDITACC.NS', 'CROMPTON.NS', 'CUMMINSIND.NS', 'CYIENT.NS',
            'DCMSHRIRAM.NS', 'DLF.NS', 'DOMS.NS', 'DABUR.NS', 'DALBHARAT.NS',
            'DATAPATTNS.NS', 'DEEPAKFERT.NS', 'DEEPAKNTR.NS', 'DELHIVERY.NS',
            'DEVYANI.NS', 'DIVISLAB.NS', 'DIXON.NS', 'LALPATHLAB.NS', 'DRREDDY.NS',
            'EIDPARRY.NS', 'EIHOTEL.NS', 'EPL.NS', 'EASEMYTRIP.NS', 'ECLERX.NS',
            'EICHERMOT.NS', 'ELECON.NS', 'ELGIEQUIP.NS', 'EMAMILTD.NS', 'ENDURANCE.NS',
            'ENGINERSIN.NS', 'EQUITASBNK.NS', 'ERIS.NS', 'ESCORTS.NS', 'EXIDEIND.NS',
            'FDC.NS', 'NYKAA.NS', 'FEDERALBNK.NS', 'FACT.NS', 'FINEORG.NS',
            'FINCABLES.NS', 'FINPIPE.NS', 'FSL.NS', 'FIVESTAR.NS', 'FORTIS.NS',
            'GAIL.NS', 'GMMPFAUDLR.NS', 'GRSE.NS', 'GICRE.NS',
            'GILLETTE.NS', 'GLAND.NS', 'GLAXO.NS', 'GLENMARK.NS',
            'MEDANTA.NS', 'GPIL.NS', 'GODFRYPHLP.NS', 'GODREJCP.NS', 'GODREJIND.NS',
            'GODREJPROP.NS', 'GRANULES.NS', 'GRAPHITE.NS', 'GRASIM.NS', 'GESHIP.NS',
            'GRINDWELL.NS', 'GAEL.NS', 'FLUOROCHEM.NS', 'GUJGASLTD.NS', 'GMDCLTD.NS',
            'GNFC.NS', 'GPPL.NS', 'GSFC.NS', 'GSPL.NS', 'HEG.NS',
            'HCLTECH.NS', 'HDFCAMC.NS', 'HDFCBANK.NS', 'HFCL.NS',
            'HAPPSTMNDS.NS', 'HAPPYFORGE.NS', 'HAVELLS.NS', 'HEROMOTOCO.NS', 'HSCL.NS',
            'HINDALCO.NS', 'HAL.NS', 'HINDCOPPER.NS', 'HINDPETRO.NS', 'HINDUNILVR.NS',
            'HINDZINC.NS', 'POWERINDIA.NS', 'HOMEFIRST.NS', 'HONASA.NS', 'HONAUT.NS',
            'HUDCO.NS', 'ICICIBANK.NS', 'ICICIGI.NS', 'ICICIPRULI.NS',
            'IDBI.NS', 'IDFCFIRSTB.NS', 'IIFL.NS', 'IRB.NS', 'IRCON.NS', 'ITC.NS',
            'ITI.NS', 'INDIACEM.NS', 'INDIAMART.NS', 'INDIANB.NS', 'IEX.NS',
            'INDHOTEL.NS', 'IOC.NS', 'IRCTC.NS', 'IRFC.NS', 'INDIGOPNTS.NS',
            'IGL.NS', 'INDUSTOWER.NS', 'INDUSINDBK.NS', 'NAUKRI.NS', 'INFY.NS',
            'INOXWIND.NS', 'INTELLECT.NS', 'INDIGO.NS', 'IPCALAB.NS', 'JBCHEPHARM.NS',
            'JKCEMENT.NS', 'JBMA.NS', 'JKLAKSHMI.NS', 'JKPAPER.NS', 'JMFINANCIL.NS',
            'JSWENERGY.NS', 'JSWSTEEL.NS', 'JINDALSTEL.NS', 'JYOTHYLAB.NS',
            'KARURVYSYA.NS', 'KOTAKBANK.NS', 'KRBL.NS', 'KSB.NS', 'KSK.NS', 'KSL.NS',
            'LAOPALA.NS', 'LT.NS', 'LICHSGFIN.NS', 'LUXIND.NS', 'M&MFIN.NS',
            'MAHSEAMLES.NS', 'MASTEK.NS', 'MAXHEALTH.NS', 'MAZDA.NS', 'MCX.NS',
            'MINDACORP.NS', 'MOLDTECH.NS', 'MOIL.NS', 'MOTILALOFS.NS',
            'NCC.NS', 'NATIONALUM.NS', 'NESTLEIND.NS', 'NDTV.NS',
            'NHPC.NS', 'NLCINDIA.NS', 'NMDC.NS', 'NTPC.NS', 'NAVINFLUOR.NS',
            'NAVNETEDUL.NS', 'NOCIL.NS', 'OIL.NS', 'OMAXE.NS', 'ONGC.NS',
            'ORIENTCEM.NS', 'ORIENTELEC.NS', 'PARAGON.NS', 'PERSISTENT.NS',
            'PAGEIND.NS', 'PAYTM.NS', 'PETRONET.NS', 'PNB.NS', 'POLYCAB.NS',
            'POWERGRID.NS', 'PPAP.NS', 'PRAKASH.NS', 'PRINCEPIPE.NS', 'RBLBANK.NS',
            'RITES.NS', 'RKFORGE.NS', 'RUBYMILLS.NS', 'SABEVENTS.NS', 'SAIL.NS',
            'SAKSOFT.NS', 'SANDHAR.NS', 'SANGAMIND.NS', 'SECL.NS', 'SFL.NS',
            'SIEMENS.NS', 'SJVN.NS', 'SKIPPER.NS', 'SOMANYCERA.NS', 'SPLIL.NS',
            'SRF.NS', 'SSWL.NS', 'SUNPHARMA.NS', 'SUPRAJIT.NS', 'TATAMOTORS.NS',
            'TATAPOWER.NS', 'TATASTEEL.NS', 'TCIEXP.NS', 'TECHM.NS', 'TITAN.NS',
            'TIMKEN.NS', 'UCOBANK.NS', 'UFO.NS', 'UJJIVANSFB.NS', 'ULTRACEMCO.NS',
            'UNIONBANK.NS', 'UPL.NS', 'VBL.NS', 'VGUARD.NS', 'VINDHYATEL.NS',
            'VOLTAS.NS', 'VSTIND.NS', 'WABAG.NS', 'WIPRO.NS', 'ZEEL.NS', '^NSEI',
            'INR=X', 'SIR=F'
        ]
        self.yahoo_tickers += [
            'AAP', 'AAT', 'ABCB', 'ABG', 'ABM', 'ABR', 'ACA', 'ACIW', 'ACLS', 'ADEA', 
            'ADTN', 'ADUS', 'AEIS', 'AEO', 'AGO', 'AGYS', 'AHCO', 'AHH', 'AIN', 'AIR', 
            'AKR', 'AL', 'ALEX', 'ALG', 'ALGT', 'ALK', 'ALKS', 'ALRM', 'AMBC', 'AMCX', 
            'AMN', 'AMPH', 'AMR', 'AMSF', 'AMWD', 'ANDE', 'ANF', 'ANIP', 'AORT', 'AOSL', 
            'APAM', 'APLE', 'APOG', 'ARCB', 'ARI', 'ARLO', 'AROC', 'ARR', 'ASIX', 
            'ASO', 'ASTE', 'ASTH', 'ATEN', 'ATGE', 'ATI', 'ATNI', 'AUB', 'AVA', 'AVAV', 
            'AVNS', 'AWI', 'AWR', 'AX', 'AXL', 'AZZ', 'B', 'BANC', 'BANF', 'BANR', 'BCC', 
            'BCPC', 'BDN', 'BFH', 'BFS', 'BGC', 'BGS', 'BHE', 'BHLB', 'BJRI', 'BKE', 
            'BKU', 'BL', 'BLMN', 'BMI', 'BOH', 'BOOT', 'BOX', 'BRC', 'BRKL', 
            'BTU', 'BXMT', 'CABO', 'CAKE', 'CAL', 'CALM', 'CALX', 'CARG', 'CARS', 
            'CASH', 'CATY', 'CBRL', 'CBU', 'CCOI', 'CCRN', 'CCS', 'CENT', 'CENTA', 
            'CENX', 'CERT', 'CEVA', 'CFFN', 'CHCO', 'CHCT', 'CHEF', 'CLB', 'CLDT', 
            'CLW', 'CMP', 'CNK', 'CNMD', 'CNS', 'CNXN', 'COHU', 'COLL', 'COOP', 
            'CORT', 'CPF', 'CPK', 'CPRX', 'CRC', 'CRK', 'CRNC', 'CRS', 'CRSR', 'CRVL', 
            'CSGS', 'CSR', 'CTKB', 'CTRE', 'CTS', 'CUBI', 'CVBF', 'CVCO', 'CVGW', 'CVI', 
            'CWEN', 'CWK', 'CWT', 'CXW', 'DAN', 'DBI', 'DCOM', 'DDD', 'DEA', 'DEI', 
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
            'POWL', 'PPBI', 'PRA', 'PRAA', 'PRDO', 'PRG', 'PRGS', 'PRK', 'PRLB', 
            'PRVA', 'PSMT', 'PTEN', 'PUMP', 'PZZA', 'QNST', 'RAMP', 'RC', 'RCUS', 'RDN', 
            'RDNT', 'RES', 'REX', 'REZI', 'RGNX', 'RGP', 'RGR', 'RILY', 'RNST', 'ROCK', 
            'ROG', 'RUN', 'RUSHA', 'RWT', 'RXO', 'SAFE', 'SABR', 'SAFT', 'SAH', 
            'SANM', 'SATS', 'SBCF', 'SBH', 'SBSI', 'SCHL', 'SCL', 'SCSC', 'SCVL', 'SDGR', 
            'SEDG', 'SEE', 'SEM', 'SFBS', 'SFNC', 'SHAK', 'SHEN', 'SHO', 'SHOO', 
            'SIG', 'SITC', 'SITM', 'SKT', 'SKYW', 'SLG', 'SLP', 'SLVM', 'SM', 
            'SMP', 'SMPL', 'SMTC', 'SNCY', 'SNEX', 'SONO', 'SPNT', 'SPSC', 'SPTN', 
            'SPXC', 'SSTK', 'STAA', 'STBA', 'STC', 'STEL', 'STRA', 'SUPN', 'SVC', 'SXC', 
            'SXI', 'SXT', 'TALO', 'TBBK', 'TFIN', 'THRM', 'THRY', 'THS', 'TILE', 
            'TMP', 'TNC', 'TNDM', 'TPH', 'TR', 'TRIP', 'TRMK', 'TRN', 'TRST', 'TRUP', 
            'TTEC', 'TTGT', 'TTMI', 'TWI', 'TWO', 'UCTT', 'UE', 'UFCS', 'UFPT', 'UHT', 
            'UNF', 'UNFI', 'UNIT', 'UPBD', 'URBN', 'USNA', 'USPH', 'UTL', 'UVV', 'VBTX', 
            'VCEL', 'VECO', 'VFC', 'VIAV', 'VICR', 'VIR', 'VRE', 'VREX', 'VRRM', 
            'VRTS', 'VSAT', 'VSCO', 'VSTS', 'VTOL', 'VTLE', 'VYX', 'WABC', 
            'WAFD', 'WD', 'WDFC', 'WGO', 'WLY', 'WNC', 'WOR', 'WRLD', 'WS', 'WSFS', 
            'WSR', 'WWW', 'XHR', 'XNCR', 'XPEL', 'XPER', 'XRX', 'YELP', 'ZEUS' 
        ]

    def load_strategies(self) -> None:
        """
        Loads the strategy outputs for productionised strategies.

        Notes:
            Production strategies with defined parameters are instantiated here.
        """
        self.bam = BAMStrategy(
            name='bam',
            lookback_window=126,
            rebal_freq=126,
            signal_update_freq=22
        ).get_strategy_output()

        self.cta = CTAStrategy(
            name='cta',
            lookback_window=126,
            rebal_freq=126,
            target_vol=0.2
        ).get_strategy_output()

        self.emm = EMMStrategy(
            name='emm',
            lookback_window=126,
            n_stocks=25,
            rebal_freq=126,
        ).get_strategy_output()

        self.newt = NEWTStrategy(
            name='newt',
            position_size=0.05,
        ).get_strategy_output()

        self.stab = STABStrategy(
            name='stab',
            n_clusters=60,
            n_sub_strategies=5
        ).get_strategy_output()
        
        self.far = FARStrategy(
            name='far'
        ).get_strategy_output()

    def create_tables(self) -> None:
        """
        Creates the necessary tables in the database if they do not already exist.
        """
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS price_data (
                symbol VARCHAR(20),
                date DATE,
                adj_close FLOAT,
                source VARCHAR(10),
                PRIMARY KEY (symbol, date)
            )
        ''')
        self.conn.commit()
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS strategy_weights (
                symbol VARCHAR(20),
                date DATE,
                effective_weight FLOAT,
                target_weight FLOAT,
                portfolio_weight FLOAT,
                strategy VARCHAR(10),
                PRIMARY KEY (symbol, date)
            )
        ''')
        self.conn.commit()

    def commit_data(self, query: str, data_tuples: list[tuple]) -> None:
        """
        Commits a list of data tuples to the database using the provided query.

        Arguments:
            query: A SQL query for committing the data tuples.
            data_tuples: A list of data tuples to be committed to the database.
        """
        BATCH_SIZE = 1000
        for i in range(0, len(data_tuples), BATCH_SIZE):
            batch = data_tuples[i:i+BATCH_SIZE]
            self.cursor.executemany(query, batch)
            self.conn.commit()

    def download_price_data(self, ticker_list: list[str]) -> pd.DataFrame:
        """
        Downloads historical price data for the given list of tickers.

        Arguments:
            ticker_list: A list of ticker symbols for historical price downloads.

        Returns:
            price_data: A DataFrame containing the downloaded price data.
        """
        tickers = {ticker: self.ticker_map[ticker]
                   for ticker in ticker_list if ticker in self.ticker_map}
        price_data = []
        for ticker, contract in tickers.items():
            historical_data = self.ib.reqHistoricalData(
                contract,
                endDateTime='' if contract.secType == 'CONTFUT' else date.today(),
                barSizeSetting='1 day',
                durationStr='10 Y',
                whatToShow='AGGTRADES' if ticker == 'BTC' else 'TRADES',
                useRTH=False
            )
            ticker_data = pd.DataFrame(historical_data)[['date', 'close']]
            ticker_data.set_index('date', inplace=True)
            ticker_data = ticker_data.rename(columns={'close': 'adj_close'})
            price_data.append(ticker_data)

        price_data = [
            ticker_data.rename_axis('Date')
            .asfreq('D')
            for ticker_data in price_data
        ]
        price_data = pd.concat(price_data, axis=1)
        weekdays = price_data.index.to_series().dt.weekday < 5
        price_data = price_data[weekdays].fillna(method='ffill')
        price_data.dropna(inplace=True)
        return price_data

    def commit_ticker_prices(self) -> None:
        """
        Commits the downloaded price data for all tickers to the database.
        """
        print('Committing ticker prices...')
        price_data = pd.DataFrame()

        for ticker in self.ticker_list:
            ticker_data = self.download_price_data([ticker])
            ticker_data['Symbol'] = ticker
            ticker_data['Source'] = 'IBKR'
            price_data = pd.concat([price_data, ticker_data])

        try:
            ticker_data = yf.download(
                self.yahoo_tickers, 
                start='2009-01-01', 
                end=date.today(), 
                progress=False,
                auto_adjust=False
            )['Adj Close']

            yahoo_price_data = (
                ticker_data.stack()
                .reset_index()
                .rename(columns={'Ticker': 'Symbol', 0: 'adj_close'})
            )
            yahoo_price_data['Date'] = pd.to_datetime(yahoo_price_data['Date'])
            yahoo_price_data = yahoo_price_data[yahoo_price_data['Date'].dt.weekday < 5]
            yahoo_price_data = yahoo_price_data.set_index('Date')
            yahoo_price_data['Source'] = 'YHOO'
            price_data = pd.concat([price_data, yahoo_price_data])

        except (ValueError, KeyError, TypeError, AttributeError) as e:
            raise(f"Error downloading Yahoo Finance data: {e}")

        price_data = list(price_data.itertuples(index=True))
        self.commit_data(self.insert_query, price_data)

    def commit_strategy_levels(self) -> None:
        """
        Commits the strategy levels to the database.
        """
        print('Committing strategy levels...')
        strategies = [
            {'levels': self.bam['Strategy Levels'], 'symbol': 'BAM'},
            {'levels': self.cta['Strategy Levels'], 'symbol': 'CTA'},
            {'levels': self.emm['Strategy Levels'], 'symbol': 'EMM'},
            {'levels': self.newt['Strategy Levels'], 'symbol': 'NEWT'},
            {'levels': self.stab['Strategy Levels'], 'symbol': 'STAB'},
            {'levels': self.far['Strategy Levels'], 'symbol': 'FAR'}
        ]
        for column in self.bam['Sub-strategy Levels'].columns:
            strategies.append({
                'levels': self.bam['Sub-strategy Levels'][[column]],
                'symbol': f'BAM.{column}'
            })
        for column in self.cta['Sub-strategy Levels'].columns:
            strategies.append({
                'levels': self.cta['Sub-strategy Levels'][[column]].copy(),
                'symbol': f'CTA.{column}'
            })
        for strategy in strategies:
            levels = strategy['levels']
            levels.index.name = 'date'
            levels.columns = ['adj_close']
            levels = levels.assign(symbol=strategy['symbol'], source='CALC')
            levels = list(levels.itertuples(index=True))
            self.commit_data(self.insert_query, levels)

    def download_portfolio_weights(self) -> pd.DataFrame:
        """
        Downloads historical portfolio weights and current positions.

        Returns:
            portfolio_weights: Historical and current portfolio weights.
        """
        query = "SELECT symbol, date, portfolio_weight FROM strategy_weights"
        self.cursor.execute(query)

        historical_weights = pd.DataFrame(
            self.cursor.fetchall(),
            columns=['symbol', 'date', 'portfolio_weight']
        )
        current_positions = [
            summary
            for summary in self.ib.accountSummary()
            if summary.tag == 'AccountPositions'
        ]
        if not current_positions:
            portfolio_weights = pd.DataFrame({
                'symbol': [ticker for ticker in self.ticker_list if ticker != 'VIX'],
                'portfolio_weight': 0
            })
            portfolio_weights['date'] = date.today()
        else:
            portfolio_weights = pd.DataFrame(current_positions)

        portfolio_weights = pd.concat(
            [historical_weights, portfolio_weights], 
            ignore_index=True
        )
        portfolio_weights['date'] = pd.to_datetime(portfolio_weights['date'])
        return portfolio_weights

    def commit_strategy_weights(self) -> None:
        """
        Commits the strategy weights to the database.
        """
        print('Committing strategy weights...')
        insert_query = (
            "INSERT INTO strategy_weights (date, symbol, effective_weight, target_weight, "
            "portfolio_weight, strategy) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE "
            "date = date, "
            "symbol = symbol, "
            "effective_weight = effective_weight, "
            "target_weight = target_weight, "
            "portfolio_weight = portfolio_weight, "
            "strategy = strategy"
        )
        strategies = [
            {'effective_weight': self.bam['Effective Weights'],
             'target_weight': self.bam['Target Weights'],
             'symbol': 'BAM'},
            {'effective_weight': self.cta['Effective Weights'],
             'target_weight': self.cta['Target Weights'],
             'symbol': 'CTA'},
            {'effective_weight': self.emm['Effective Weights'],
             'target_weight': self.emm['Target Weights'],
             'symbol': 'EMM'},
            {'effective_weight': self.stab['Effective Weights'],
             'target_weight': self.stab['Target Weights'],
             'symbol': 'STAB'},
            {'effective_weight': self.far['Effective Weights'],
             'target_weight': self.far['Target Weights'],
             'symbol': 'FAR'}
        ]
        for strategy in strategies:
            effective_weights = strategy['effective_weight']
            effective_weights.index.name = 'date'
            effective_weights = (
                effective_weights
                .reset_index()
                .melt(id_vars='date', value_name='effective_weight', var_name='symbol')
            )
            target_weights = strategy['target_weight']
            target_weights.index.name = 'date'
            target_weights = (
                target_weights
                .reset_index()
                .melt(id_vars='date', value_name='target_weight', var_name='symbol')
            )
            portfolio_weights = self.download_portfolio_weights()
            strategy_weights = (
                effective_weights
                .merge(target_weights, on=['date', 'symbol'], how='left')
                .merge(portfolio_weights, on=['date', 'symbol'], how='left')
            )
            strategy_weights = strategy_weights.set_index('date')
            strategy_weights['strategy'] = strategy['symbol']
            strategy_weights.fillna(0, inplace=True)
            strategy_weights = list(strategy_weights.itertuples(index=True))
            self.commit_data(insert_query, strategy_weights)

    def commit_nav(self) -> None:
        """
        Commits the Net Asset Value (NAV) of the portfolio to the database.
        """
        print('Committing portfolio NAV...')
        total_assets = float(next(
            item.value for item in self.ib.accountSummary() if item.tag == 'NetLiquidation'))
        nav = pd.DataFrame(
            {'adj_close': [total_assets]}, index=[date.today()])
        nav.index.name = 'date'
        nav['symbol'] = 'ITK'
        nav['source'] = 'IBKR'
        nav = list(nav.itertuples(index=True))
        self.commit_data(self.insert_query, nav)

    def run_updates(self) -> None:
        """
        Runs the update process ('live' commits live data; 'test' skips data commit).
        """
        if self.run_mode == 'live':
            self.commit_ticker_prices()
            self.commit_nav()
            self.commit_strategy_levels()
            self.commit_strategy_weights()
            print('Data commit has been completed.')
            ut.print_separator()
            self.ib.disconnect()
        elif self.run_mode == 'test':
            pass
        else:
            raise ValueError('Invalid run type...')
        ut.close_db(self.conn, self.cursor)
