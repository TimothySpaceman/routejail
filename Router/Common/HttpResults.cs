namespace Router.Common;

public static class HttpResults
{
    public static IResult ToHttpResult<T>(ServiceResult<T> result, Func<T, IResult>? onSuccess = null)
    {
        return result.Status switch
        {
            ServiceResultStatus.Ok => onSuccess != null ? onSuccess(result.Value!) : Results.Ok(result.Value),
            ServiceResultStatus.NotFound => Results.NotFound(new { error = result.ErrorMessage }),
            ServiceResultStatus.Invalid => Results.BadRequest(new { error = result.ErrorMessage }),
            ServiceResultStatus.Conflict => Results.Conflict(new { error = result.ErrorMessage }),
            _ => Results.StatusCode(500)
        };
    }

    public static IResult ToHttpResult(ServiceResult result, Func<IResult>? onSuccess = null)
    {
        return result.Status switch
        {
            ServiceResultStatus.Ok => onSuccess != null ? onSuccess() : Results.NoContent(),
            ServiceResultStatus.NotFound => Results.NotFound(new { error = result.ErrorMessage }),
            ServiceResultStatus.Invalid => Results.BadRequest(new { error = result.ErrorMessage }),
            ServiceResultStatus.Conflict => Results.Conflict(new { error = result.ErrorMessage }),
            _ => Results.StatusCode(500)
        };
    }
}
