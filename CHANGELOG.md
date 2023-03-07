# About
We follow semantic versioning.

# Template
```
# Release 0.0.0 (YYYY-MM-DD)
* **New Ops**
  * Connector Hooks
    * ...
  * Connector Interfaces
    * ...
  * Generators
    * ...
  * Consumers
    * ...
* **Breaking Changes**
  * ...
* **Non-breaking Changes**
  * ...
* **Notes from future**
  * ...
```
# Release 0.3.1 (2023-03-07)
* **Breaking Changes**
  * Bugfix/Improvement: Improved backoff in HTTPConnOp. `backoff_config: {}` must be added to config file to enable backoff with default values.

# Release 0.3.0 (2023-03-07)
* **New Ops**
  * Generators
    * `EvalExprConditionalOp`
* **Non-breaking Changes**
  * Added `time` and `logger` to the EVAL_GLOBALS

# Release 0.2.0 (2023-03-06)
* **New Ops**
  * Connector Hooks
    * `PythonListHookConnOp`
  * Connector Interfaces
    * `ListConnOp`
  * Generators
    * `BladeProductsListVariationsOp`
    * `BladeProductsViewVariationsOp`
    * `GetListGeneratorOp`
  * Consumers
    * `PutListConsumerOp`
* **Non-breaking Changes**
  * Added `.gitignore` in config folder

# Release 0.1.0 (2023-02-28)
* **New Ops**
  * Connector Hooks
    * ...
  * Connector Interfaces
    * ...
  * Generators
    * ...
  * Consumers
    * ...
