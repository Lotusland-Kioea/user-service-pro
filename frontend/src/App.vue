<template>
  <div class="app-container">
    <h1 style="text-align: center; margin-bottom: 20px;">用户管理系统</h1>

    <!-- 搜索栏 -->
    <div class="toolbar">
      <el-input
        v-model="keyword"
        placeholder="搜索姓名或邮箱"
        clearable
        style="width: 300px; margin-right: 10px;"
        @keyup.enter="handleSearch"
      />
      <el-button type="primary" @click="handleSearch">
        <el-icon style="margin-right: 4px;"><Search /></el-icon>搜索
      </el-button>
      <el-button type="success" @click="openCreateDialog">
        <el-icon style="margin-right: 4px;"><Plus /></el-icon>新增用户
      </el-button>
    </div>

    <!-- 数据表格 -->
    <el-table :data="tableData" border stripe v-loading="loading" style="width: 100%; margin-top: 16px;">
      <el-table-column prop="id" label="ID" width="80" align="center" />
      <el-table-column prop="name" label="姓名" width="150" />
      <el-table-column prop="email" label="邮箱" min-width="200" />
      <el-table-column prop="createdAt" label="创建时间" width="180" />
      <el-table-column prop="updatedAt" label="更新时间" width="180" />
      <el-table-column label="操作" width="180" align="center" fixed="right">
        <template #default="{ row }">
          <el-button size="small" type="primary" @click="openEditDialog(row)">编辑</el-button>
          <el-popconfirm title="确定删除该用户吗？" @confirm="handleDelete(row.id)">
            <template #reference>
              <el-button size="small" type="danger">删除</el-button>
            </template>
          </el-popconfirm>
        </template>
      </el-table-column>
    </el-table>

    <!-- 分页 -->
    <div class="pagination">
      <el-pagination
        v-model:current-page="currentPage"
        v-model:page-size="pageSize"
        :page-sizes="[5, 10, 20, 50]"
        :total="total"
        layout="total, sizes, prev, pager, next, jumper"
        @size-change="loadData"
        @current-change="loadData"
      />
    </div>

    <!-- 新增/编辑对话框 -->
    <el-dialog
      v-model="dialogVisible"
      :title="isEdit ? '编辑用户' : '新增用户'"
      width="500px"
      :close-on-click-modal="false"
    >
      <el-form
        ref="formRef"
        :model="form"
        :rules="formRules"
        label-width="80px"
      >
        <el-form-item label="姓名" prop="name">
          <el-input v-model="form.name" placeholder="请输入姓名" maxlength="100" />
        </el-form-item>
        <el-form-item label="邮箱" prop="email">
          <el-input v-model="form.email" placeholder="请输入邮箱" maxlength="200" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="submitting" @click="handleSubmit">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, reactive, onMounted } from 'vue'
import { ElMessage } from 'element-plus'
import { Search, Plus } from '@element-plus/icons-vue'
import { userApi } from './api/userApi'

const loading = ref(false)
const submitting = ref(false)
const tableData = ref([])
const total = ref(0)
const currentPage = ref(1)
const pageSize = ref(10)
const keyword = ref('')

const dialogVisible = ref(false)
const isEdit = ref(false)
const editId = ref(null)
const formRef = ref(null)

const form = reactive({
  name: '',
  email: ''
})

const formRules = {
  name: [
    { required: true, message: '请输入姓名', trigger: 'blur' },
    { min: 1, max: 100, message: '姓名长度在 1 到 100 个字符', trigger: 'blur' }
  ],
  email: [
    { required: true, message: '请输入邮箱', trigger: 'blur' },
    { type: 'email', message: '请输入正确的邮箱格式', trigger: 'blur' }
  ]
}

async function loadData() {
  loading.value = true
  try {
    const result = await userApi.list({
      page: currentPage.value,
      size: pageSize.value,
      keyword: keyword.value || undefined
    })
    tableData.value = result.records || []
    total.value = result.total || 0
  } catch (e) {
    ElMessage.error(e.message || '加载数据失败')
  } finally {
    loading.value = false
  }
}

function handleSearch() {
  currentPage.value = 1
  loadData()
}

function openCreateDialog() {
  isEdit.value = false
  editId.value = null
  form.name = ''
  form.email = ''
  dialogVisible.value = true
  // Reset validation
  setTimeout(() => formRef.value?.resetFields(), 0)
}

function openEditDialog(row) {
  isEdit.value = true
  editId.value = row.id
  form.name = row.name
  form.email = row.email
  dialogVisible.value = true
  setTimeout(() => formRef.value?.clearValidate(), 0)
}

async function handleSubmit() {
  const valid = await formRef.value.validate().catch(() => false)
  if (!valid) return

  submitting.value = true
  try {
    if (isEdit.value) {
      await userApi.update(editId.value, { name: form.name, email: form.email })
      ElMessage.success('更新成功')
    } else {
      await userApi.create({ name: form.name, email: form.email })
      ElMessage.success('创建成功')
    }
    dialogVisible.value = false
    loadData()
  } catch (e) {
    ElMessage.error(e.message || '操作失败')
  } finally {
    submitting.value = false
  }
}

async function handleDelete(id) {
  try {
    await userApi.delete(id)
    ElMessage.success('删除成功')
    // If last item on current page, go to previous page
    if (tableData.value.length === 1 && currentPage.value > 1) {
      currentPage.value--
    }
    loadData()
  } catch (e) {
    ElMessage.error(e.message || '删除失败')
  }
}

onMounted(() => {
  loadData()
})
</script>

<style>
body {
  margin: 0;
  background: #f5f7fa;
}
.app-container {
  max-width: 1000px;
  margin: 0 auto;
  padding: 20px;
}
.toolbar {
  display: flex;
  align-items: center;
}
.pagination {
  margin-top: 16px;
  display: flex;
  justify-content: flex-end;
}
</style>
