FROM oven/bun:1.3.14-alpine
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --ignore-scripts
COPY tsconfig.json tsconfig.build.json ./
COPY contracts ./contracts
COPY src ./src
RUN bun run build
ENV HOST=0.0.0.0 PORT=3037 APP_ROLES=http,workers
EXPOSE 3037
CMD ["bun", "dist/main.js"]
