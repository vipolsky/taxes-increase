from roverdata.db import DataWarehouse
from taxes_increase.queries import queries_dict
from taxes_increase import utils

def main():
    logger = utils.setup_logger(__name__)
    conn = DataWarehouse()

    logger.info('fetching LTV data')
    LTV = conn.query(queries_dict["LTV"])
    logger.info('fetching retransaction data')
    retrans = conn.query(queries_dict["retrans"])

    return LTV, retrans