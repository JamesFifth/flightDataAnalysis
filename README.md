# flightDataAnalysis

In this project, my group develop an Oozie workflow to process and analyze a large volume of flight data.

We use three MapReduce programs and run them in fully distributed on AWS EC2 clusters

We run the workflow to analyze the entire data set (total 22 years from 1987 to 2008) on two VMs first and then gradually increase the system scale to the maximum allowed number of VMs for at least 5 increment steps, and measure each corresponding workflow execution time.

Run the workflow to analyze the data in a progressive manner with an increment of 1 year, i.e. the first year (1987), the first 2 years (1987-1988), the first 3 years (1987-1989), ..., and the total 22 years (1987-2008), on the maximum allowed number of VMs, and measured each corresponding workflow execution time.
