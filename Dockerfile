FROM node:20-slim

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY tsconfig.json ./
COPY src ./src

EXPOSE 3000

# Overridden per-service in docker-compose.yml
CMD ["npm", "run", "start:server"]