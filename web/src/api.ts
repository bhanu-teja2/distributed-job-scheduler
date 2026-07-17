export type Job={id:string;name:string;job_type:string;status:string;priority:number;run_at:string;retry_count:number;max_retries:number;last_error?:string;payload:unknown;created_at:string;updated_at:string}
export type Page<T>={items:T[];page:number;page_size:number;total:number}
type Envelope<T>={success:boolean;data:T;error?:{code:string;message:string};request_id:string}
export class ApiError extends Error{constructor(public code:string,message:string,public status:number){super(message)}}
export const getKey=()=>sessionStorage.getItem('scheduler_api_key')||''
export const setKey=(value:string)=>value?sessionStorage.setItem('scheduler_api_key',value):sessionStorage.removeItem('scheduler_api_key')
export async function api<T>(path:string,options:RequestInit={}):Promise<T>{const response=await fetch(path,{...options,headers:{'Content-Type':'application/json','X-API-Key':getKey(),...(options.headers||{})}});const body=await response.json() as Envelope<T>;if(!response.ok||!body.success)throw new ApiError(body.error?.code||'REQUEST_FAILED',body.error?.message||'Request failed',response.status);return body.data}
