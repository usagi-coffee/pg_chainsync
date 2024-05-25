FROM oven/bun:1

WORKDIR /app
COPY . .
RUN bun i

ENV NODE_ENV=production
USER bun
ENTRYPOINT [ "bun", "run", "index.ts" ]
