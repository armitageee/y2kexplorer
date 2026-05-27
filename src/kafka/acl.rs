//! Kafka ACL admin API via librdkafka FFI (not exposed in rdkafka 0.38 high-level API).

use std::ffi::{CStr, CString};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use rdkafka::bindings::{
    self as rdsys, rd_kafka_AclBindingFilter_t, rd_kafka_AclBinding_t, rd_kafka_AclOperation_t,
    rd_kafka_AclPermissionType_t, rd_kafka_AdminOptions_t, rd_kafka_ResourcePatternType_t,
    rd_kafka_ResourceType_t, rd_kafka_t,
};

const ERR_BUF: usize = 512;
const POLL_MS: i32 = 100;
const TIMEOUT_MS: i32 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclEntry {
    pub resource_type: String,
    pub resource_name: String,
    pub pattern_type: String,
    pub principal: String,
    pub host: String,
    pub operation: String,
    pub permission: String,
}

impl AclEntry {
    pub fn to_spec(&self) -> AclSpec {
        AclSpec {
            resource_type: self.resource_type.clone(),
            resource_name: self.resource_name.clone(),
            pattern_type: self.pattern_type.clone(),
            principal: self.principal.clone(),
            host: self.host.clone(),
            operation: self.operation.clone(),
            permission: self.permission.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AclSpec {
    pub resource_type: String,
    pub resource_name: String,
    pub pattern_type: String,
    pub principal: String,
    pub host: String,
    pub operation: String,
    pub permission: String,
}

pub fn list_acls(rk: *mut rd_kafka_t) -> Result<Vec<AclEntry>> {
    let filter = new_filter_any()?;
    let _filter_guard = BindingFilterGuard(filter);
    let result = admin_op(
        rk,
        rdsys::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBEACLS,
        rdsys::RD_KAFKA_EVENT_DESCRIBEACLS_RESULT,
        |rk, opts, queue| unsafe {
            rdsys::rd_kafka_DescribeAcls(rk, filter, opts, queue);
        },
        |ev| unsafe {
            let res = rdsys::rd_kafka_event_DescribeAcls_result(ev);
            if res.is_null() {
                return Err(anyhow!("DescribeAcls: null result"));
            }
            let mut cnt: usize = 0;
            let ptrs = rdsys::rd_kafka_DescribeAcls_result_acls(res, &mut cnt);
            parse_acl_bindings(ptrs, cnt)
        },
    )?;
    Ok(result)
}

pub fn create_acl(rk: *mut rd_kafka_t, spec: &AclSpec) -> Result<()> {
    let binding = new_binding(spec)?;
    let _binding_guard = BindingGuard(binding);
    let mut arr = [binding];
    admin_op(
        rk,
        rdsys::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_CREATEACLS,
        rdsys::RD_KAFKA_EVENT_CREATEACLS_RESULT,
        |rk, opts, queue| unsafe {
            rdsys::rd_kafka_CreateAcls(rk, arr.as_mut_ptr(), arr.len(), opts, queue);
        },
        |ev| unsafe {
            let res = rdsys::rd_kafka_event_CreateAcls_result(ev);
            if res.is_null() {
                return Err(anyhow!("CreateAcls: null result"));
            }
            let mut cnt: usize = 0;
            let results = rdsys::rd_kafka_CreateAcls_result_acls(res, &mut cnt);
            if results.is_null() || cnt == 0 {
                return Ok(());
            }
            for i in 0..cnt {
                let aclres = *results.add(i);
                let err = rdsys::rd_kafka_acl_result_error(aclres);
                if !err.is_null() {
                    return Err(kafka_error(err));
                }
            }
            Ok(())
        },
    )?;
    Ok(())
}

pub fn delete_acl(rk: *mut rd_kafka_t, spec: &AclSpec) -> Result<usize> {
    let filter = new_binding_filter(spec)?;
    let _filter_guard = BindingFilterGuard(filter);
    let mut arr = [filter];
    let deleted = admin_op(
        rk,
        rdsys::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DELETEACLS,
        rdsys::RD_KAFKA_EVENT_DELETEACLS_RESULT,
        |rk, opts, queue| unsafe {
            rdsys::rd_kafka_DeleteAcls(rk, arr.as_mut_ptr(), arr.len(), opts, queue);
        },
        |ev| unsafe {
            let res = rdsys::rd_kafka_event_DeleteAcls_result(ev);
            if res.is_null() {
                return Err(anyhow!("DeleteAcls: null result"));
            }
            let mut cnt: usize = 0;
            let responses = rdsys::rd_kafka_DeleteAcls_result_responses(res, &mut cnt);
            let mut total = 0usize;
            for i in 0..cnt {
                let resp = *responses.add(i);
                let err = rdsys::rd_kafka_DeleteAcls_result_response_error(resp);
                if !err.is_null() {
                    return Err(kafka_error(err));
                }
                let mut mc: usize = 0;
                let matching =
                    rdsys::rd_kafka_DeleteAcls_result_response_matching_acls(resp, &mut mc);
                total += mc;
                if !matching.is_null() && mc > 0 {
                    // bindings are const pointers; nothing to free per doc
                    let _ = parse_acl_bindings(matching, mc)?;
                }
            }
            Ok(total)
        },
    )?;
    Ok(deleted)
}

/// Kafka has no in-place ACL update — delete the old binding and create the new one.
pub fn replace_acl(rk: *mut rd_kafka_t, old: &AclSpec, new: &AclSpec) -> Result<()> {
    let n = delete_acl(rk, old)?;
    if n == 0 {
        return Err(anyhow!(
            "no ACL matched the previous binding (nothing deleted)"
        ));
    }
    create_acl(rk, new).context("create ACL after delete")
}

fn admin_op<T, F, G>(
    rk: *mut rd_kafka_t,
    op: rdsys::rd_kafka_admin_op_t,
    event_type: i32,
    call: F,
    parse: G,
) -> Result<T>
where
    F: FnOnce(*mut rd_kafka_t, *const rd_kafka_AdminOptions_t, *mut rdsys::rd_kafka_queue_t),
    G: FnOnce(*mut rdsys::rd_kafka_event_t) -> Result<T>,
{
    unsafe {
        let queue = rdsys::rd_kafka_queue_new(rk);
        if queue.is_null() {
            return Err(anyhow!("rd_kafka_queue_new failed"));
        }

        let result = (|| {
            let mut err_buf = vec![0i8; ERR_BUF];
            let opts = rdsys::rd_kafka_AdminOptions_new(rk, op);
            if opts.is_null() {
                return Err(anyhow!("AdminOptions_new failed"));
            }
            let _opts_guard = AdminOptionsGuard(opts);

            let res = rdsys::rd_kafka_AdminOptions_set_request_timeout(
                opts,
                TIMEOUT_MS,
                err_buf.as_mut_ptr(),
                err_buf.len(),
            );
            if res != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(anyhow!("set_request_timeout: {}", err_buf_to_str(&err_buf)));
            }

            call(rk, opts, queue);

            let ev = poll_event(queue, event_type)?;
            let _ev_guard = EventGuard(ev);

            let err = rdsys::rd_kafka_event_error(ev);
            if err != rdsys::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                let msg = cstr_opt(rdsys::rd_kafka_event_error_string(ev))
                    .unwrap_or_else(|| format!("{err:?}"));
                return Err(anyhow!("admin event error: {msg}"));
            }

            parse(ev)
        })();

        rdsys::rd_kafka_queue_destroy(queue);
        result
    }
}

struct AdminOptionsGuard(*const rd_kafka_AdminOptions_t);
impl Drop for AdminOptionsGuard {
    fn drop(&mut self) {
        unsafe {
            rdsys::rd_kafka_AdminOptions_destroy(self.0 as *mut _);
        }
    }
}

struct EventGuard(*mut rdsys::rd_kafka_event_t);
impl Drop for EventGuard {
    fn drop(&mut self) {
        unsafe {
            rdsys::rd_kafka_event_destroy(self.0);
        }
    }
}

struct BindingGuard(*mut rd_kafka_AclBinding_t);
impl Drop for BindingGuard {
    fn drop(&mut self) {
        unsafe {
            rdsys::rd_kafka_AclBinding_destroy(self.0);
        }
    }
}

struct BindingFilterGuard(*mut rd_kafka_AclBindingFilter_t);
impl Drop for BindingFilterGuard {
    fn drop(&mut self) {
        unsafe {
            rdsys::rd_kafka_AclBinding_destroy(self.0);
        }
    }
}

unsafe fn poll_event(
    queue: *mut rdsys::rd_kafka_queue_t,
    expect_type: i32,
) -> Result<*mut rdsys::rd_kafka_event_t> {
    let deadline = std::time::Instant::now() + Duration::from_millis(TIMEOUT_MS as u64);
    loop {
        let ev = rdsys::rd_kafka_queue_poll(queue, POLL_MS);
        if !ev.is_null() {
            let typ = rdsys::rd_kafka_event_type(ev);
            if typ == expect_type {
                return Ok(ev);
            }
            if typ == rdsys::RD_KAFKA_EVENT_ERROR {
                let err = rdsys::rd_kafka_event_error(ev);
                let msg = cstr_opt(rdsys::rd_kafka_event_error_string(ev))
                    .unwrap_or_else(|| format!("{err:?}"));
                rdsys::rd_kafka_event_destroy(ev);
                return Err(anyhow!("queue error event: {msg}"));
            }
            rdsys::rd_kafka_event_destroy(ev);
        }
        if std::time::Instant::now() >= deadline {
            return Err(anyhow!("admin operation timed out"));
        }
    }
}

fn new_filter_any() -> Result<*mut rd_kafka_AclBindingFilter_t> {
    new_binding_filter_wildcard(
        rdsys::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_ANY,
        None,
        rdsys::rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_ANY,
        None,
        None,
        rdsys::rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ANY,
        rdsys::rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ANY,
    )
}

fn new_binding(spec: &AclSpec) -> Result<*mut rd_kafka_AclBinding_t> {
    let (restype, name) = parse_resource(&spec.resource_type, &spec.resource_name)?;
    let pattern = parse_pattern_type(&spec.pattern_type)?;
    let operation = parse_operation(&spec.operation)?;
    let permission = parse_permission(&spec.permission)?;
    let principal = CString::new(spec.principal.as_str()).context("principal")?;
    let host = CString::new(if spec.host.is_empty() {
        "*"
    } else {
        &spec.host
    })
    .context("host")?;
    let name_c = CString::new(name).context("resource name")?;

    let mut err_buf = vec![0i8; ERR_BUF];
    let ptr = unsafe {
        rdsys::rd_kafka_AclBinding_new(
            restype,
            name_c.as_ptr(),
            pattern,
            principal.as_ptr(),
            host.as_ptr(),
            operation,
            permission,
            err_buf.as_mut_ptr(),
            err_buf.len(),
        )
    };
    if ptr.is_null() {
        return Err(anyhow!("AclBinding_new: {}", err_buf_to_str(&err_buf)));
    }
    Ok(ptr)
}

fn new_binding_filter(spec: &AclSpec) -> Result<*mut rd_kafka_AclBindingFilter_t> {
    let (restype, name) = parse_resource_filter(&spec.resource_type, &spec.resource_name)?;
    let pattern = parse_pattern_type_filter(&spec.pattern_type)?;
    let operation = parse_operation_filter(&spec.operation)?;
    let permission = parse_permission_filter(&spec.permission)?;
    let name_opt = filter_name_ptr(restype, &name)?;
    let principal_opt = filter_optional_cstr(&spec.principal, "principal")?;
    let host_opt = filter_optional_cstr(&spec.host, "host")?;
    new_binding_filter_wildcard(
        restype,
        name_opt.as_ref(),
        pattern,
        principal_opt.as_ref(),
        host_opt.as_ref(),
        operation,
        permission,
    )
}

/// Сборка AclBindingFilter. Для «любое значение» в librdkafka нужен **NULL**, не `""`.
fn new_binding_filter_wildcard(
    restype: rd_kafka_ResourceType_t,
    name: Option<&CString>,
    pattern: rd_kafka_ResourcePatternType_t,
    principal: Option<&CString>,
    host: Option<&CString>,
    operation: rd_kafka_AclOperation_t,
    permission: rd_kafka_AclPermissionType_t,
) -> Result<*mut rd_kafka_AclBindingFilter_t> {
    let name_ptr = name.map(|c| c.as_ptr()).unwrap_or(std::ptr::null());
    let principal_ptr = principal.map(|c| c.as_ptr()).unwrap_or(std::ptr::null());
    let host_ptr = host.map(|c| c.as_ptr()).unwrap_or(std::ptr::null());

    let mut err_buf = vec![0i8; ERR_BUF];
    let ptr = unsafe {
        rdsys::rd_kafka_AclBindingFilter_new(
            restype,
            name_ptr,
            pattern,
            principal_ptr,
            host_ptr,
            operation,
            permission,
            err_buf.as_mut_ptr(),
            err_buf.len(),
        )
    };
    if ptr.is_null() {
        return Err(anyhow!(
            "AclBindingFilter_new: {}",
            err_buf_to_str(&err_buf)
        ));
    }
    Ok(ptr)
}

fn filter_name_ptr(restype: rd_kafka_ResourceType_t, name: &str) -> Result<Option<CString>> {
    if name.is_empty() && restype == rdsys::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_ANY {
        return Ok(None);
    }
    Ok(Some(CString::new(name).context("resource name")?))
}

fn filter_optional_cstr(value: &str, field: &'static str) -> Result<Option<CString>> {
    let v = value.trim();
    if v.is_empty() || v == "*" {
        return Ok(None);
    }
    CString::new(v).with_context(|| field.to_string()).map(Some)
}

unsafe fn parse_acl_bindings(
    ptrs: *mut *const rd_kafka_AclBinding_t,
    cnt: usize,
) -> Result<Vec<AclEntry>> {
    if ptrs.is_null() || cnt == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(cnt);
    for i in 0..cnt {
        let acl = *ptrs.add(i);
        let err = rdsys::rd_kafka_AclBinding_error(acl);
        if !err.is_null() {
            return Err(kafka_error(err));
        }
        out.push(parse_acl_binding(acl)?);
    }
    out.sort_by(|a, b| {
        (
            &a.resource_type,
            &a.resource_name,
            &a.principal,
            &a.operation,
        )
            .cmp(&(
                &b.resource_type,
                &b.resource_name,
                &b.principal,
                &b.operation,
            ))
    });
    Ok(out)
}

unsafe fn parse_acl_binding(acl: *const rd_kafka_AclBinding_t) -> Result<AclEntry> {
    let restype = rdsys::rd_kafka_AclBinding_restype(acl);
    let resource_type = cstr_name(rdsys::rd_kafka_ResourceType_name(restype))
        .unwrap_or_else(|| format!("{restype:?}"));

    let pattern = rdsys::rd_kafka_AclBinding_resource_pattern_type(acl);
    let pattern_type = cstr_name(rdsys::rd_kafka_ResourcePatternType_name(pattern))
        .unwrap_or_else(|| format!("{pattern:?}"));

    let op = rdsys::rd_kafka_AclBinding_operation(acl);
    let operation =
        cstr_name(rdsys::rd_kafka_AclOperation_name(op)).unwrap_or_else(|| format!("{op:?}"));

    let perm = rdsys::rd_kafka_AclBinding_permission_type(acl);
    let permission = cstr_name(rdsys::rd_kafka_AclPermissionType_name(perm))
        .unwrap_or_else(|| format!("{perm:?}"));

    Ok(AclEntry {
        resource_type,
        resource_name: cstr_opt(rdsys::rd_kafka_AclBinding_name(acl)).unwrap_or_default(),
        pattern_type,
        principal: cstr_opt(rdsys::rd_kafka_AclBinding_principal(acl)).unwrap_or_default(),
        host: cstr_opt(rdsys::rd_kafka_AclBinding_host(acl)).unwrap_or_default(),
        operation,
        permission,
    })
}

unsafe fn kafka_error(err: *const rdsys::rd_kafka_error_t) -> anyhow::Error {
    let msg = cstr_opt(rdsys::rd_kafka_error_string(err)).unwrap_or_else(|| "unknown".into());
    anyhow!("{msg}")
}

fn cstr_opt(ptr: *const std::os::raw::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .ok()
        .map(str::to_string)
}

fn cstr_name(ptr: *const std::os::raw::c_char) -> Option<String> {
    cstr_opt(ptr).map(|s| s.to_ascii_lowercase())
}

fn err_buf_to_str(buf: &[i8]) -> String {
    let end = buf.iter().position(|&c| c == 0).unwrap_or(buf.len());
    let bytes: Vec<u8> = buf[..end].iter().map(|&c| c as u8).collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

fn parse_resource(ty: &str, name: &str) -> Result<(rd_kafka_ResourceType_t, String)> {
    let t = ty.trim().to_ascii_lowercase();
    match t.as_str() {
        "topic" => Ok((
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC,
            name.to_string(),
        )),
        "group" => Ok((
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_GROUP,
            name.to_string(),
        )),
        "broker" => Ok((
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER,
            name.to_string(),
        )),
        "transactional_id" | "transactional-id" | "txn" => Ok((
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TRANSACTIONAL_ID,
            name.to_string(),
        )),
        "cluster" => Ok((
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_BROKER,
            if name.is_empty() {
                "kafka-cluster".into()
            } else {
                name.to_string()
            },
        )),
        _ => Err(anyhow!(
            "resource_type: expected topic|group|broker|cluster|transactional_id"
        )),
    }
}

fn parse_resource_filter(ty: &str, name: &str) -> Result<(rd_kafka_ResourceType_t, String)> {
    let t = ty.trim().to_ascii_lowercase();
    if t == "any" || t == "*" {
        return Ok((
            rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_ANY,
            name.to_string(),
        ));
    }
    parse_resource(ty, name)
}

fn parse_pattern_type(s: &str) -> Result<rd_kafka_ResourcePatternType_t> {
    match s.trim().to_ascii_lowercase().as_str() {
        "literal" => Ok(rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_LITERAL),
        "prefixed" | "prefix" => {
            Ok(rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_PREFIXED)
        }
        "match" => Ok(rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_MATCH),
        _ => Err(anyhow!("pattern_type: literal|prefixed|match")),
    }
}

fn parse_pattern_type_filter(s: &str) -> Result<rd_kafka_ResourcePatternType_t> {
    let t = s.trim().to_ascii_lowercase();
    if t == "any" || t == "*" {
        return Ok(rd_kafka_ResourcePatternType_t::RD_KAFKA_RESOURCE_PATTERN_ANY);
    }
    parse_pattern_type(s)
}

fn parse_operation(s: &str) -> Result<rd_kafka_AclOperation_t> {
    match s.trim().to_ascii_lowercase().as_str() {
        "all" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALL),
        "read" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_READ),
        "write" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_WRITE),
        "create" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CREATE),
        "delete" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DELETE),
        "alter" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER),
        "describe" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE),
        "cluster_action" | "cluster" => {
            Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_CLUSTER_ACTION)
        }
        "describe_configs" => {
            Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_DESCRIBE_CONFIGS)
        }
        "alter_configs" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ALTER_CONFIGS),
        "idempotent_write" => Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_IDEMPOTENT_WRITE),
        _ => Err(anyhow!(
            "operation: all|read|write|create|delete|alter|describe|cluster_action|describe_configs|alter_configs|idempotent_write"
        )),
    }
}

fn parse_operation_filter(s: &str) -> Result<rd_kafka_AclOperation_t> {
    let t = s.trim().to_ascii_lowercase();
    if t == "any" || t == "*" {
        return Ok(rd_kafka_AclOperation_t::RD_KAFKA_ACL_OPERATION_ANY);
    }
    parse_operation(s)
}

fn parse_permission(s: &str) -> Result<rd_kafka_AclPermissionType_t> {
    match s.trim().to_ascii_lowercase().as_str() {
        "allow" => Ok(rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ALLOW),
        "deny" => Ok(rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_DENY),
        _ => Err(anyhow!("permission: allow|deny")),
    }
}

fn parse_permission_filter(s: &str) -> Result<rd_kafka_AclPermissionType_t> {
    let t = s.trim().to_ascii_lowercase();
    if t == "any" || t == "*" {
        return Ok(rd_kafka_AclPermissionType_t::RD_KAFKA_ACL_PERMISSION_TYPE_ANY);
    }
    parse_permission(s)
}
