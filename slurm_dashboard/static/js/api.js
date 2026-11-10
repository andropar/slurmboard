// Server access. Writes carry a custom header: the server rejects cross-site
// requests, which cannot set it.

export class ApiError extends Error {
    constructor(message, status) {
        super(message);
        this.status = status;
    }
}

async function request(method, path, body) {
    const init = { method, headers: {} };
    if (method !== 'GET') init.headers['X-Slurm-Dashboard'] = '1';
    if (body !== undefined) {
        init.headers['Content-Type'] = 'application/json';
        init.body = JSON.stringify(body);
    }

    let response;
    try {
        response = await fetch(path, init);
    } catch (err) {
        throw new ApiError('The dashboard server did not respond', 0);
    }
    let data = null;
    try {
        data = await response.json();
    } catch (err) {
        // Non-JSON body (proxy error page, empty reply): handled by the status check.
    }
    if (!response.ok) {
        throw new ApiError((data && data.error) || `Request failed (HTTP ${response.status})`, response.status);
    }
    return data;
}

export const api = {
    get: path => request('GET', path),
    post: (path, body = {}) => request('POST', path, body),
    del: path => request('DELETE', path),
};

/** Job IDs can contain brackets ("12_[1-5]"), so always encode them in paths. */
export const jobPath = id => encodeURIComponent(id);
