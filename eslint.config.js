'use strict';

const {FlatCompat} = require('@eslint/eslintrc');

const compat = new FlatCompat({baseDirectory: __dirname});

// The ESLint ecmaVersion argument is inconsistently used. Some rules will ignore it entirely, so if the rule has
// been set, it will still error even if it's not applicable to that version number. Since Google sets these
// rules, we have to turn them off ourselves.
const DISABLED_ES6_OPTIONS = {
  'no-var': 'off',
  'prefer-rest-params': 'off'
};

const SHAREDB_RULES = {
  // Comma dangle is not supported in ES3
  'comma-dangle': ['error', 'never'],
  // We control our own objects and prototypes, so no need for this check
  'guard-for-in': 'off',
  // Google prescribes different indents for different cases. Let's just use 2 spaces everywhere. Note that we have
  // to override ESLint's default of 0 indents for this.
  'indent': ['error', 2, {
    'SwitchCase': 1
  }],
  // Less aggressive line length than Google, which is especially useful when we have a lot of callbacks in our code
  'max-len': ['error',
    {
      code: 120,
      tabWidth: 2,
      ignoreUrls: true
    }
  ],
  // Google overrides the default ESLint behaviour here, which is slightly better for catching erroneously unused variables
  'no-unused-vars': ['error', {vars: 'all', args: 'after-used'}],
  // It's more readable to ensure we only have one statement per line
  'max-statements-per-line': ['error', {max: 1}],
  // ES3 doesn't support spread
  'prefer-spread': 'off',
  // as-needed quote props are easier to write
  'quote-props': ['error', 'as-needed'],
  'require-jsdoc': 'off',
  'valid-jsdoc': 'off'
};

module.exports = [
  {
    ignores: ['coverage/', 'eslint.config.js']
  },
  ...compat.extends('google'),
  {
    // The source targets ES3/ES5 (no ES6 syntax), but the parser is set to ES5 because some code accesses reserved
    // words as property names (e.g. Promise `.catch()`), which is only valid from ES5 onwards. ES3-incompatible
    // constructs (trailing commas, spread, etc.) are still caught by the explicit rules below.
    languageOptions: {
      ecmaVersion: 5,
      sourceType: 'script'
    },
    rules: Object.assign(
      {},
      DISABLED_ES6_OPTIONS,
      SHAREDB_RULES
    )
  }
];
