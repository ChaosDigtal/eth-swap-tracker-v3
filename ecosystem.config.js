module.exports = {
  apps: [
    {
      name: 'webhooks-example',
      script: 'npx',
      args: 'ts-node --transpile-only src/index.ts',
      watch: true,
      ignore_watch: ['logs'], // Ignore changes in the logs directory
      max_restarts: 20,
      output: './logs/out.log',
      error: './logs/error.log',
      log: './logs/combined.outerr.log',
      env: {
        NODE_ENV: 'development',
      },
      env_production: {
        NODE_ENV: 'production',
      },
    },
  ],
};
