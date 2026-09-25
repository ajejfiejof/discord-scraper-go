module.exports = {
  extends: ['@commitlint/config-conventional'],
  rules: {
    'type-enum': [2, 'always', ['feat', 'fix', 'docs', 'chore', 'refactor', 'perf', 'test', 'ci', 'revert', 'build', 'style']],
    'scope-enum': [1, 'always', ['writer', 'ratelimit', 'discord', 'extractor', 'config', 'checkpoint', 'scraper', 'docs', 'license', 'deps', 'ci', 'gha']],
    'subject-case': [2, 'never', ['sentence-case', 'start-case', 'pascal-case', 'upper-case']],
    'header-max-length': [2, 'always', 100],
    'body-max-line-length': [2, 'always', 100],
  },
};
