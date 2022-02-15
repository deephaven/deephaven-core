FROM node:16 as build
WORKDIR /usr/src/packages
# COPY package.json /usr/src/packages/package.json
# RUN set -eux; \
#     npm install --legacy-peer-deps

COPY load-plugins.sh /usr/src/packages/load-plugins.sh
COPY plugins.txt /usr/src/packages/plugins.txt

RUN set -eux; \
    ./load-plugins.sh

# TODO: Create the manifest... iterate through all the dependencies and add their package.json to the manifest... so the manifest is literally just an amalgamation of all the package.jsons?? Yea I think that works.
# Could even be just an array...
# RUN set -eux; \
#     npm pack @deephaven/matplotlib-js-plugin@0.9.5-alpha.4; \
#     tar --touch -xf deephaven-matplotlib-js-plugin-0.9.5-alpha.4.tgz

FROM deephaven/web:local-build
COPY --from=build /usr/src/packages/js-plugins/ /usr/share/nginx/html/js-plugins/