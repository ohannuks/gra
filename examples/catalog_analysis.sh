# Get all strain data, and PE samples
gra data get lvk all 
# Process an example strain data (noise analysis) - produces PSD and noise model samples
gra data process GW231123_135430
# Do PE on all events: --reuse will reuse PE samples from previous runs, if available, to save time
gra analyse vanilla all --reuse
# Do the regular vanilla population analysis
gra population vanilla
# Do the milli/microlensing analyses for all events, again reusing samples if available
gra analyse millilensing all --reuse --bbhpop=vanilla
gra analyse microlensing all --reuse --bbhpop=vanilla
# Analyse the example strain specifically:
gra analyse vanilla GW231123_135430 --noise=student-t
gra analyse millilensing GW231123_135430 --noise=student-t
# Do the strong lensing analysis for all events
gra analyse strong all --posterioroverlap --weedcandidates --bbhpop=vanilla --lenspop=default
# Now do the population-level analysis; limited to top 20 most likely hypotheses.
gra population all --top20
# Print summary of results
gra summary all





