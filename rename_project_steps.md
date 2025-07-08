# 项目重命名为 data-diff-n8n 的完整步骤

## 已完成的文件更新

✅ 已更新以下文件中的项目名称和仓库引用：
- `pyproject.toml` - 项目名称和仓库URL
- `README.md` - 项目标题和GitHub链接
- `docs/conf.py` - 文档项目名称
- `CONTRIBUTING.md` - GitHub仓库链接
- `docs/new-database-driver-guide.rst` - 贡献指南链接
- `docs/index.rst` - 文档中的仓库链接
- `data_diff/dbt_parser.py` - 错误报告链接

## 完整的重命名流程

### 推荐的执行顺序：

1. **先完成文件内容更新** ✅ (已完成)
2. **在 GitHub 创建新仓库**
3. **更新 Git 远程地址并推送**
4. **重命名本地文件夹**

### 详细步骤：

### 1. 在 GitHub 上创建新仓库
1. 访问 https://github.com/new
2. 仓库名称设为：`data-diff-n8n`
3. 设置为 Public（或根据需要选择 Private）
4. 不要初始化 README，因为本地已有文件
5. 点击"Create repository"

### 2. 更新本地 Git 仓库
在项目根目录执行以下命令：

```bash
# 添加新的远程仓库
git remote set-url origin https://github.com/yunqiqiliang/data-diff-n8n.git

# 或者如果你使用 SSH
git remote set-url origin git@github.com:yunqiqiliang/data-diff-n8n.git

# 查看当前远程仓库配置
git remote -v

# 提交所有更改
git add .
git commit -m "Rename project to data-diff-n8n"

# 推送到新仓库
git push -u origin main
```

### 3. 验证更改
1. 访问新的仓库页面：https://github.com/yunqiqiliang/data-diff-n8n
2. 检查 README.md 是否正确显示
3. 确认所有链接都指向新仓库

### 4. 重命名本地文件夹
建议在完成 Git 操作后重命名本地文件夹：

```bash
# 在父目录中执行
cd /Users/liangmo/Documents/GitHub/
mv data-diff-qiliang data-diff-n8n
cd data-diff-n8n
```

**注意：** 重命名文件夹后，如果你的编辑器或IDE中有打开的项目，需要重新打开新路径下的项目。

## 注意事项

- 确保你有权限创建新的 GitHub 仓库
- 如果原仓库是 fork，新仓库将是一个独立的仓库
- 记得更新任何本地开发环境中的路径引用
- 考虑通知团队成员仓库地址的变更

## 完成后的验证清单

- [ ] 新 GitHub 仓库已创建
- [ ] 本地 Git 远程地址已更新
- [ ] 代码已推送到新仓库
- [ ] 本地文件夹已重命名为 data-diff-n8n
- [ ] 在新路径下重新打开项目
- [ ] README.md 正确显示项目新名称
- [ ] 所有文档链接指向新仓库
- [ ] 项目可以正常构建和运行
