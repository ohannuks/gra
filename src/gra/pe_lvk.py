from gra import data_lvk
#_get_lvk_pe_data_filename(event_name)


if __name__ == "__main__":
    event_name = "GW231123_135430"
    data_lvk._get_lvk_pe_data(event_name) # Download the data if it does not already exist
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    print(f"Posterior filename for event '{event_name}': {posterior_filename}")

