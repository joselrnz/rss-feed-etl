# Security Guidelines

## 🔒 Confidential Information Protection

This repository contains job hunting automation tools that may handle sensitive personal and professional information. Follow these guidelines to protect your data.

## ⚠️ NEVER COMMIT THESE FILES

### 🔑 Credentials & API Keys
- `secrets/service_account.json` - Google Sheets API credentials
- `config/.env*` - Environment files with real values
- Any file containing API keys, tokens, or passwords

### 📊 Personal Data
- `resume.pdf` / `resume.docx` - Your personal resume
- `cv.pdf` / `cv.docx` - Your curriculum vitae
- Personal contact information files
- Job application tracking data

### 🗂️ Spreadsheet Information
- Google Spreadsheet IDs
- Sheet names that might reveal personal info
- Any configuration files with real spreadsheet references

### 📝 Logs & Temporary Files
- `logs/*.log` - May contain personal data from processing
- Temporary files with real data
- Cache files with sensitive information

## ✅ SAFE TO COMMIT

### 📋 Template Files
- `config/.env.example` - Template with placeholder values
- `examples/*.yaml` - Example configurations
- Documentation files
- Source code (without hardcoded secrets)

### 🔧 Configuration Templates
- YAML files with placeholder values
- Example RSS feed configurations
- Documentation and guides

## 🛡️ Security Best Practices

### 1. Environment Variables
```bash
# Use environment variables for sensitive data
export GOOGLE_SPREADSHEET_ID="your_actual_id"
export OPENROUTER_API_KEY="your_actual_key"
```

### 2. Separate Development/Production
```bash
# Development environment
cp config/.env.example config/.env.development
# Edit with dev values

# Production environment  
cp config/.env.example config/.env.production
# Edit with prod values
```

### 3. File Permissions
```bash
# Restrict access to sensitive files
chmod 600 config/.env*
chmod 600 secrets/service_account.json
chmod 600 resume.pdf
```

### 4. Git Configuration
```bash
# Check what's being tracked
git status

# Remove sensitive files from tracking
git rm --cached config/.env.development
git rm --cached secrets/service_account.json

# Commit the removal
git commit -m "Remove sensitive files from tracking"
```

## 🔍 Security Checklist

Before committing code:

- [ ] No real API keys in code
- [ ] No spreadsheet IDs in committed files
- [ ] No personal information in committed files
- [ ] Environment files use placeholder values
- [ ] .gitignore covers all sensitive patterns
- [ ] Logs directory is ignored
- [ ] Resume/CV files are ignored

## 🚨 If You Accidentally Commit Secrets

### 1. Immediate Actions
```bash
# Remove from latest commit
git reset --soft HEAD~1
git reset HEAD config/.env.development
git commit -m "Remove sensitive configuration"

# Or remove specific files
git rm --cached secrets/service_account.json
git commit -m "Remove credentials from tracking"
```

### 2. Clean Git History (if needed)
```bash
# For files committed in multiple commits
git filter-branch --force --index-filter \
  'git rm --cached --ignore-unmatch secrets/service_account.json' \
  --prune-empty --tag-name-filter cat -- --all
```

### 3. Rotate Compromised Credentials
- Generate new Google service account key
- Regenerate API keys
- Update all environment files

## 📋 Environment File Template

Use this template for your environment files:

```bash
# Copy config/.env.example to config/.env.development
cp config/.env.example config/.env.development

# Edit with your actual values
nano config/.env.development
```

## 🔧 Automated Security Checks

The repository includes security measures:

1. **Comprehensive .gitignore** - Blocks common sensitive files
2. **Template files** - Safe examples without real data
3. **Documentation** - Clear guidelines for safe usage

## 📞 Security Questions?

If you're unsure whether something is safe to commit:
1. Check if it contains real personal data
2. Check if it contains API keys or credentials
3. Check if it contains spreadsheet IDs
4. When in doubt, don't commit it

## 🎯 Remember

**The goal is to share the automation tools while keeping your personal job hunting data private and secure.**

---

**Stay secure and happy job hunting!** 🔒🚀
