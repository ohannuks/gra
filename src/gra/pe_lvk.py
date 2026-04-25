from gra import data_lvk
import deepdish as dd # Goodbye h5py hell

def read_config_from_posterior(event_name):
    import deepdish as dd
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    with h5py.File(posterior_filename, 'r') as f:

def build_likelihood(event_name):
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    config = read_config_from_posterior(event_name)

if __name__ == "__main__":
    event_name = "GW231123_135430"
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    print(f"Posterior filename for event '{event_name}': {posterior_filename}")
    

