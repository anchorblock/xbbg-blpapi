import pandas as pd

from functools import partial
from itertools import product
from contextlib import contextmanager

from xbbg import __version__, const, pipeline
from xbbg.io import logs, files, storage
from xbbg.core import utils, conn, process
from xbbg.core.conn import connect
from blpapi import Name

NAME_RESULTS = Name("results")


__all__ = [
    '__version__',
    'connect',
    'bdp',
    'bds',
    'bdh',
    'bdib',
    'bdtick',
    'earning',
    'dividend',
    'beqs',
    'live',
    'subscribe',
    'adjust_ccy',
    'turnover',
]


# def bdp(tickers, flds, **kwargs) -> pd.DataFrame:
def instruments(query,maxResults, **kwargs) -> pd.DataFrame :
    """
    Bloomberg instruments data

    Args:
        query: query_string
        maxResults: maxResults
        **kwargs: Bloomberg overrides

    Returns:
        pd.DataFrame
    """

    # Set default values if not provided
    if query is None:
        query = "Dhaka"
        print("Default query used: 'default_query'")
    
    if maxResults is None:
        maxResults = 10  # Set your desired default value
        print(f"Default maxResults used: {maxResults}")

    return pd.DataFrame(get_instruments(query,maxResults),
                        columns=['Security','Description'])


# def bdp(tickers, flds, **kwargs) -> pd.DataFrame:
def curveList(query,maxResults, **kwargs) -> pd.DataFrame :
    """
    Bloomberg instruments data

    Args:
        query: query_string
        maxResults: maxResults
        **kwargs: Bloomberg overrides

    Returns:
        pd.DataFrame
    """

    # Set default values if not provided
    if query is None:
        query = "SOFR"
        print("Default query used: 'default_query'")
    
    if maxResults is None:
        maxResults = 10  # Set your desired default value
        print(f"Default maxResults used: {maxResults}")

    return pd.DataFrame(get_curveList(query,maxResults),
                        columns=['curve','description','country', 'currency', 'curveid', 'type', 'subType'])


                        
# def bdp(tickers, flds, **kwargs) -> pd.DataFrame:
def govList(query,maxResults, **kwargs) -> pd.DataFrame :
    """
    Bloomberg instruments data

    Args:
        query: query_string
        maxResults: maxResults
        **kwargs: Bloomberg overrides

    Returns:
        pd.DataFrame
    """

    # Set default values if not provided
    if query is None:
        query = "Bangladesh"
        print("Default query used: 'default_query'")
    
    if maxResults is None:
        maxResults = 10  # Set your desired default value
        print(f"Default maxResults used: {maxResults}")

    return pd.DataFrame(get_govermentList(query,maxResults),
                        columns=['ParseKey','Name', 'Ticker'])

def get_instruments(query, maxResults): 
    req = process.create_request(service='//blp/instruments', request='instrumentListRequest')
    req.set('query', query)
    req.set('maxResults', maxResults)

    def _process_instruments(msg): #Process the response
        for elt in msg.asElement().getElement('results').values():
            security = elt.getElementAsString('security')
            description = elt.getElementAsString('description')
            yield security, description

    conn.send_request(request=req)
    return process.rec_events(func=_process_instruments)


def get_curveList(query, maxResults): 
    req = process.create_request(service='//blp/instruments', request='curveListRequest')
    req.set('query', query)
    req.set('maxResults', maxResults)

    def _process_instruments(msg): #Process the response

        results = msg[NAME_RESULTS]
        for i in range(results.numValues()):
            curve = ""
            description = ""
            country = ""
            currency = ""
            curveid = ""
            ctype = ""
            csubType = ""
            result = results.getValueAsElement(i)
            for j in range(result.numElements()):
                field = result.getElement(j)
                key = field.name()
                if key == 'subtype':
                    values = [field.getValueAsString(k) for k in range(field.numValues())]
                    value_str = ", ".join(values)
                    csubType = value_str
                elif key == 'type':
                    values = [field.getValueAsString(k) for k in range(field.numValues())]
                    value_str = ", ".join(values)
                    ctype = value_str
                else:
                    # Handle non-array values
                    value_str = field.getValueAsString()
                    if key == 'curve': 
                        curve = value_str
                    elif key == 'description':
                        description = value_str
                    if key == 'country': 
                        country = value_str
                    elif key == 'currency':
                        currency = value_str
                    elif key == 'curveid':
                        curveid = value_str
            yield curve, description, country, currency, curveid, ctype , csubType

    conn.send_request(request=req)
    return process.rec_events(func=_process_instruments)



def get_govermentList(query, maxResults): 
    req = process.create_request(service='//blp/instruments', request='govtListRequest')
    req.set('query', query)
    req.set('maxResults', maxResults)

    def _process_instruments(msg): #Process the response
        for elt in msg.asElement().getElement('results').values():
            parse_key = elt.getElementAsString('parseky')
            name = elt.getElementAsString('name')
            ticker = elt.getElementAsString('ticker')
            yield parse_key, name, ticker

    conn.send_request(request=req)
    return process.rec_events(func=_process_instruments)


def bds(tickers, flds, use_port=False, **kwargs) -> pd.DataFrame:
    """
    Bloomberg block data

    Args:
        tickers: ticker(s)
        flds: field
        use_port: use `PortfolioDataRequest`
        **kwargs: other overrides for query

    Returns:
        pd.DataFrame: block data
    """
    logger = logs.get_logger(bds, **kwargs)

    part = partial(_bds_, fld=flds, logger=logger, use_port=use_port, **kwargs)
    if isinstance(tickers, str): tickers = [tickers]
    return pd.DataFrame(pd.concat(map(part, tickers), sort=False))


def _bds_(
        ticker: str,
        fld: str,
        logger: logs.logging.Logger,
        use_port: bool = False,
        **kwargs,
) -> pd.DataFrame:
    """
    Get data of BDS of single ticker
    """
    if 'has_date' not in kwargs: kwargs['has_date'] = True
    data_file = storage.ref_file(ticker=ticker, fld=fld, ext='pkl', **kwargs)
    if files.exists(data_file):
        logger.debug(f'Loading Bloomberg data from: {data_file}')
        return pd.DataFrame(pd.read_pickle(data_file))

    request = process.create_request(
        service='//blp/refdata',
        request='PortfolioDataRequest' if use_port else 'ReferenceDataRequest',
        **kwargs,
    )
    process.init_request(request=request, tickers=ticker, flds=fld, **kwargs)
    logger.debug(f'Sending request to Bloomberg ...\n{request}')
    conn.send_request(request=request, **kwargs)

    res = pd.DataFrame(process.rec_events(func=process.process_ref, **kwargs))
    if kwargs.get('raw', False): return res
    if res.empty or any(fld not in res for fld in ['ticker', 'field']):
        return pd.DataFrame()

    data = (
        res
        .set_index(['ticker', 'field'])
        .droplevel(axis=0, level=1)
        .rename_axis(index=None)
        .pipe(pipeline.standard_cols, col_maps=kwargs.get('col_maps', None))
    )
    if data_file:
        logger.debug(f'Saving Bloomberg data to: {data_file}')
        files.create_folder(data_file, is_file=True)
        data.to_pickle(data_file)

    return data

