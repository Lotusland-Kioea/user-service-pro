import axios from 'axios'

const api = axios.create({
  baseURL: '/api',
  timeout: 10000
})

// Response interceptor — unwrap ApiResponse
api.interceptors.response.use(
  response => {
    const body = response.data
    if (body.code !== 200) {
      return Promise.reject(new Error(body.message || '请求失败'))
    }
    return body.data
  },
  error => {
    const msg = error.response?.data?.message || error.message || '网络错误'
    return Promise.reject(new Error(msg))
  }
)

export default api
