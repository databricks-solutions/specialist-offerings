# Contributing to Databricks Specialist Solutions Architect Content Repository

We welcome contributions to this repository! This document outlines the process for contributing new content, code assets, and improvements to the Databricks SSA Content Repository.

## Getting Started

All contributions are welcome for updates and improvements. However, for new contributions, we recommend checking with the specialization lead before contributing to ensure alignment with current priorities and avoid duplication of effort.

This repo serves as an index of code offered by Databricks Specialists, and these Offerings take 2 forms:
- Code lives OUTSIDE this repo: at minimum, you can create a new folder in the specialization area with a [catalog-listing.yml](templates/catalog-listing-template.yml) in it to point internal tooling to the code offering.
- Code lives IN this repo: if you want to maintain and version your code in this repo, you can do that as well! Create a folder in the specialization area for the offering and add the required assets as described below. 

## Contribution Requirements

All new contributions must meet the following requirements:

### 1. Ownership Structure
- **Primary Owner**: The main contributor responsible for the content
- **Secondary Owner**: A backup owner who can maintain the content if the primary owner is unavailable
- Both owners should be clearly identified in the contribution's README
- Owners are expected to review content on a recurring basis to make updates, improvements, and sunset legacy code or approaches if necessary

### 2. Code Quality Standards
- **Unit Tests**: All supporting code must include comprehensive unit tests. For reference: https://docs.databricks.com/aws/en/notebooks/testing
- **Multi-cloud Testing**: Include configurations and settings for all clouds where the code can be run, and test thoroughly on each.
- **Code Documentation**: Include docstrings and inline comments where appropriate

### 3. Documentation Requirements
- **README**: Every contribution must include a README following the template structure that explains:
  - Purpose and objectives
  - Prerequisites and setup instructions
  - Usage examples and workflows
  - Expected outcomes and deliverables
- **Dependencies**: All library dependencies must be documented in either:
  - The contribution's README
  - A requirements.txt file within the contribution
- **Top-level Documentation**: Any libraries used must be added to the top-level README libraries section

### 4. Branch Management
- Create a feature branch with a descriptive name related to the new contribution
- Use clear, descriptive branch names (e.g., `feature/data-quality-framework`, `feature/genai-rag-template`)
- Keep branches focused on a single contribution or related set of changes

## Contribution Process

### 1. Pre-Contribution Checklist
- [ ] Contact the specialization lead for new contributions
- [ ] Ensure no duplicate work exists
- [ ] Verify alignment with repository goals

### 2. Development Process
- [ ] Create a feature branch from `main`
- [ ] Implement your contribution following the requirements above
- [ ] Add comprehensive unit tests
- [ ] Create or update documentation
- [ ] Update the top-level README if new libraries are introduced
- [ ] Add a [catalog-listing.yml](templates/catalog-listing-template.yml) file so the content can be indexed
- [ ] Test your contribution thoroughly

### 3. Pull Request Process
- [ ] Create a pull request with a clear description
- [ ] Include both primary and secondary owners in the PR
- [ ] Ensure all CI/CD checks pass
- [ ] Request review from at least 2 approvers
- [ ] Address all feedback before merging

## Repository Standards

### Code Organization
- Follow the established folder structure (`data-warehousing`, `data-engineering`, `gen-ai`, `cybersecurity`)
- Use the templates in `/templates` for consistency
- Maintain clear separation between different types of content

### Documentation Standards
- Use clear, concise language
- Include practical examples
- Provide step-by-step instructions
- Document all dependencies and requirements

### Testing Standards
- Write tests for all new functionality
- Include both unit tests and integration tests where appropriate
- Ensure tests are maintainable and well-documented

## Review Process

This repository requires **2 approvers** before merging to the main branch. Reviewers will check for:

- Compliance with contribution requirements
- Code quality and test coverage
- Documentation completeness
- Alignment with repository standards
- Proper dependency management


## License

By contributing to this repository, you agree that your contributions will be licensed under the same license as the repository.

---

Thank you for contributing to the Databricks SSA Content Repository!
