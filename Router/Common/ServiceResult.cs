namespace Router.Common;

public enum ServiceResultStatus
{
    Ok,
    NotFound,
    Invalid,
    Conflict
}

public record ServiceResult(ServiceResultStatus Status, string? ErrorMessage = null)
{
    public bool IsSuccess => Status == ServiceResultStatus.Ok;

    public static ServiceResult Ok() => new(ServiceResultStatus.Ok);
    public static ServiceResult NotFound(string message = "Not found") => new(ServiceResultStatus.NotFound, message);
    public static ServiceResult Invalid(string message) => new(ServiceResultStatus.Invalid, message);
    public static ServiceResult Conflict(string message) => new(ServiceResultStatus.Conflict, message);
}

public record ServiceResult<T>(ServiceResultStatus Status, T? Value = default, string? ErrorMessage = null)
{
    public bool IsSuccess => Status == ServiceResultStatus.Ok;

    public static ServiceResult<T> Ok(T value) => new(ServiceResultStatus.Ok, value);
    public static ServiceResult<T> NotFound(string message = "Not found") => new(ServiceResultStatus.NotFound, default, message);
    public static ServiceResult<T> Invalid(string message) => new(ServiceResultStatus.Invalid, default, message);
    public static ServiceResult<T> Conflict(string message) => new(ServiceResultStatus.Conflict, default, message);
}
