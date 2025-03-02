FROM node:20.3.1

WORKDIR /app

COPY ./src /app/src
# package and tsconfig
COPY ./*.json /app/

COPY ./custom-transform.ts /app/src/transform.ts

RUN npm ci && npm run build

CMD ["node", "/app/build/src/main.js"]