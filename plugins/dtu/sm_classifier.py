from tqdm import tqdm

from threephi_framework.data_extractor.data_extractor import DataExtractor
from threephi_framework.controllers.meta import MetaController
import threephi_framework.db.db as threephi_db

def meter_evaluation(sm_ids):

    # Initialize classification dicts
    sm_classification_chunk = {}
    meta_controller = MetaController(threephi_db.new_session)

    for sm_id in tqdm(sm_ids, desc="Characterizing smart meters"):
    # TODO: Classify based on the classification from meta controller
    
        sm_characterization = meta_controller.get_sm_characterization(sm_id)

        sm_classification_chunk[str(sm_id)] = sm_characterization
    
    return sm_classification_chunk