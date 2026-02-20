# Create an injection catalog with 300 events
gra data inject lvk vanilla --n_events=300 --cbcpop=default --noise=simulated
gra data inject lvk strong --n_events=10 --cbcpop=default --lenspop=default --noise=simulated
gra data inject multimessenger --n_events=1 --cbcpop=default --lenspop=default --sourcepop=default --noise=simulated
# Do PE on all events
gra analyse vanilla all
# Do the regular vanilla population analysis
gra population vanilla
# Do the milli/microlensing analyses for all events, again reusing samples if available
gra analyse millilensing all --reuse --cbcpop=vanilla
gra analyse microlensing all --reuse --cbcpop=vanilla
# Do the strong lensing analysis for all events
gra analyse strong all --posterioroverlap --weedcandidates --cbcpop=vanilla --lenspop=default
# Now do the population-level analysis; limited to top 20 most likely hypotheses.
gra population all --top20
# Print summary of results
gra summary all





