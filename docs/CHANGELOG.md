# Changelog

All notable changes to this project will be documented in this file.

## [1.0.6] - 2025-02-23
### Added
- Created far.py.
- Created test_far.py.

### Changed
- Added FAR strategy to factory.py.

## [1.0.5] - 2024-11-24
### Added
- Created stab.py.
- Created test_stab.py.

### Changed
- Added NEWT and STAB strategies to factory.py.

## [1.0.4] - 2024-11-15
### Added
- Created newt.py.
- Created test_newt.py.

## [1.0.3] - 2024-11-01
### Added
- Created unit tests.

## [1.0.2] - 2024-10-27
### Added
- Created strategy.py.
- Created utils.py.

### Changed
- Renamed index.py to factory.py.
- Refactored project structure.
- Reorganized strategy modules to inherit from abstract base class.

## [1.0.1] - 2024-10-01
### Added
- Integrated type hinting.
- Created __init__.py.

### Changed
- Refactored modules to PEP-8.

### Fixed
- Amended hedging logic in emm.py.

## [1.0.0] - 2024-08-18
### Added
- Initial release of the **ithaka** project.
- Introduced six modules: `main`, `bam`, `cta`, `emm`, `index`, and `tracker`.
- Added functionality to handle MySQL database integration.
- Developed a Dash app for tracking live strategy calculations and prompting required trades.
