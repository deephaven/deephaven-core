## Real-Time Fraud Detection

### About

This repository contains a real-time credit card fraud detection algorithm implemented in [Deephaven Community Core](https://github.com/deephaven/deephaven-core).

The credit card purchase data in this application comes from [this Kaggle dataset](https://www.kaggle.com/mlg-ulb/creditcardfraud).  It is one of the most popular data sets on all of [Kaggle](https://www.kaggle.com/).

It uses DBSCAN to identify fraudulent purchases as spatial outliers amongst valid ones in the set.  A four hour window is used to test the validity of the idea, then a "DBSCAN in real-time" model is applied, which operates on a four-hour window of data that occurs 24 hours after the first window.

### Requirements

- [docker](https://www.docker.com/)
- [docker-compose](https://docs.docker.com/compose/)

### Launch

To run this application, you can either get started with Deephaven by doing [install Deephaven from pre-built images](https://deephaven.io/core/docs/tutorials/quickstart/) or [build from source](https://deephaven.io/core/docs/how-to-guides/launch-build/).  If you choose this route, you will have to [add Python packages](https://deephaven.io/core/docs/how-to-guides/install-python-packages/) that the code uses.  You can also clone this repository, and run the shell file found in the base directory.
