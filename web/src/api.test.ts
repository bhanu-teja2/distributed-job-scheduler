import {describe,expect,it} from 'vitest'
import {ApiError} from './api'
describe('ApiError',()=>{it('preserves structured API error details',()=>{const error=new ApiError('FORBIDDEN','denied',403);expect(error.code).toBe('FORBIDDEN');expect(error.status).toBe(403);expect(error.message).toBe('denied')})})
