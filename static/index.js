const JOB_TYPES = ["scheduled", "enqueued", "processing", "processed", "failed"]
const PAGE_SIZES = [25, 50, 100]
const REFRESH_INTERVAL_MS = 10_000

const appState = {
    ...readUrlState(),
    capabilities: {
        readOnly: true,
        jobQuery: false,
        retryFailed: false,
    },
    jobs: [],
    jobsLoaded: false,
    jobsLoading: false,
    hasMore: false,
    lastUpdatedAt: null,
    lastRefreshFailed: false,
    selectedJob: null,
    selectedJobType: null,
    refreshing: false,
    refreshQueued: false,
    requestID: 0,
    detailTrigger: null,
    detailCloseFocus: null,
}

const DETAILS_COLUMN = {label: "Details", value: renderDetailsAction}

const JOB_COLUMNS = {
    scheduled: [
        {label: "Name", value: renderJobName},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Scheduled for", value: job => formatTime(job.enqueueAt), muted: true},
        {label: "Created", value: job => formatTime(job.scheduledAt), muted: true},
        DETAILS_COLUMN,
    ],
    enqueued: [
        {label: "Name", value: renderJobName},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Retries", value: job => escapeHtml(job.retryCount ?? 0)},
        {label: "Enqueued", value: job => formatTime(job.enqueuedAt), muted: true},
        DETAILS_COLUMN,
    ],
    processing: [
        {label: "Name", value: renderJobName},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Started", value: job => formatTime(job.startedAt), muted: true},
        {
            label: "Runtime",
            value: job => `<span data-runtime-start="${escapeHtml(job.startedAt || "")}">${formatRuntime(job.startedAt)}</span>`,
            muted: true,
        },
        DETAILS_COLUMN,
    ],
    processed: [
        {label: "Name", value: renderJobName},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Completed", value: job => formatTime(job.completedAt), muted: true},
        {label: "Runtime", value: job => formatRuntime(job.startedAt, job.completedAt), muted: true},
        DETAILS_COLUMN,
    ],
    failed: [
        {label: "Name", value: renderJobName},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Error", value: job => `<span class="error-summary">${escapeHtml(getErrorParts(job).message)}</span>`},
        {label: "Retries", value: job => escapeHtml(job.retryCount ?? 0)},
        {label: "Next retry", value: job => formatTime(job.nextRetryAt), muted: true},
        DETAILS_COLUMN,
    ],
}

document.addEventListener("DOMContentLoaded", async () => {
    populateControlsFromState()
    bindEvents()
    updateSelectedState()
    renderLoadingMessage()

    await refreshCapabilities()
    normalizeFiltersForCapabilities({replaceUrl: true})
    applyCapabilityState()
    await refreshAll()

    window.setInterval(() => {
        updateRuntimeValues()
        updateSyncStatusText()
    }, 1_000)

    window.setInterval(() => {
        const autoRefresh = document.querySelector("#auto-refresh")
        if (autoRefresh?.checked && !document.hidden) {
            refreshAll({background: true})
        }
    }, REFRESH_INTERVAL_MS)
})

function bindEvents() {
    document.querySelectorAll("[data-job-filter]").forEach(link => {
        link.addEventListener("click", event => {
            event.preventDefault()
            selectJobType(link.dataset.jobFilter, {focusHeading: true})
        })
    })

    document.querySelector("#refresh-button")?.addEventListener("click", () => refreshAll())

    document.querySelector("#job-filters")?.addEventListener("submit", event => {
        event.preventDefault()
        const search = document.querySelector("#job-search")
        const queue = document.querySelector("#queue-filter")
        const pageSize = document.querySelector("#page-size")

        appState.q = appState.capabilities.jobQuery ? search?.value.trim() || "" : ""
        appState.queue = appState.capabilities.jobQuery || appState.jobType === "enqueued"
            ? queue?.value.trim() || ""
            : ""
        appState.limit = parsePageSize(pageSize?.value)
        appState.offset = 0
        pushUrlState()
        loadSelectedState()
    })

    document.querySelector("#page-size")?.addEventListener("change", () => {
        const form = document.querySelector("#job-filters")
        form?.requestSubmit()
    })

    document.querySelector("#clear-filters")?.addEventListener("click", () => {
        appState.q = ""
        appState.queue = ""
        appState.offset = 0
        populateControlsFromState()
        pushUrlState()
        loadSelectedState()
    })

    document.querySelector("#previous-page")?.addEventListener("click", () => {
        if (appState.jobsLoading) {
            return
        }
        appState.offset = Math.max(0, appState.offset - appState.limit)
        pushUrlState()
        loadSelectedState({focusHeading: true})
    })

    document.querySelector("#next-page")?.addEventListener("click", () => {
        if (appState.jobsLoading || !appState.hasMore) {
            return
        }
        appState.offset += appState.limit
        pushUrlState()
        loadSelectedState({focusHeading: true})
    })

    document.querySelector("#jobs-table-body")?.addEventListener("click", event => {
        const button = event.target.closest("[data-job-id]")
        if (!button) {
            return
        }
        openJobDetails(button.dataset.jobId, button)
    })

    document.querySelector("#job-details-body")?.addEventListener("click", event => {
        const copyButton = event.target.closest("[data-copy]")
        if (copyButton) {
            copyJobValue(copyButton.dataset.copy)
        }
    })

    document.querySelector("#retry-job-button")?.addEventListener("click", showRetryConfirmation)
    document.querySelector("#cancel-retry")?.addEventListener("click", hideRetryConfirmation)
    document.querySelector("#confirm-retry")?.addEventListener("click", retrySelectedJob)

    document.querySelector("#job-details")?.addEventListener("hidden.bs.offcanvas", () => {
        const trigger = appState.detailTrigger
        const fallback = appState.detailCloseFocus
        hideRetryConfirmation({restoreFocus: false})
        appState.selectedJob = null
        appState.selectedJobType = null
        appState.detailTrigger = null
        appState.detailCloseFocus = null
        if (trigger?.isConnected) {
            trigger.focus()
        } else if (fallback && !fallback.hidden) {
            fallback.focus()
        } else {
            document.querySelector("#selected-state-title")?.focus({preventScroll: true})
        }
    })

    document.addEventListener("visibilitychange", () => {
        const autoRefresh = document.querySelector("#auto-refresh")
        if (!document.hidden && autoRefresh?.checked) {
            refreshAll({background: true})
        }
    })

    window.addEventListener("popstate", () => {
        const nextState = readUrlState()
        if (nextState.jobType !== appState.jobType) {
            closeOpenJobDetails()
        }
        Object.assign(appState, nextState)
        normalizeFiltersForCapabilities({replaceUrl: true})
        populateControlsFromState()
        updateSelectedState()
        applyCapabilityState()
        loadSelectedState()
    })
}

async function refreshCapabilities() {
    try {
        const response = await fetch("api/capabilities", {headers: {Accept: "application/json"}})
        if (!response.ok) {
            throw new Error("Could not load dashboard capabilities.")
        }
        const body = await response.json()
        const capabilities = body.data || {}
        appState.capabilities = {
            readOnly: capabilities.readOnly !== false,
            jobQuery: capabilities.jobQuery === true,
            retryFailed: capabilities.retryFailed === true,
        }
    } catch (error) {
        appState.capabilities = {readOnly: true, jobQuery: false, retryFailed: false}
    }
}

function applyCapabilityState() {
    const mode = document.querySelector("#dashboard-mode")
    const actionsEnabled = !appState.capabilities.readOnly && appState.capabilities.retryFailed
    if (mode) {
        mode.textContent = actionsEnabled ? "Actions enabled" : "Read-only"
        mode.classList.toggle("actions-enabled", actionsEnabled)
    }

    const search = document.querySelector("#job-search")
    const queue = document.querySelector("#queue-filter")
    const canSearch = appState.capabilities.jobQuery
    const canFilterQueue = canSearch || appState.jobType === "enqueued"
    if (search) {
        search.disabled = !canSearch
    }
    if (queue) {
        queue.disabled = !canFilterQueue
        queue.placeholder = canSearch
            ? "All queues"
            : appState.jobType === "enqueued" ? "Default queue" : "Queue filter unavailable"
    }

    const note = document.querySelector("#query-capability-note")
    if (note) {
        note.hidden = canSearch
    }
}

async function refreshAll({background = false} = {}) {
    if (appState.refreshing) {
        appState.refreshQueued = true
        return null
    }

    appState.refreshing = true
    const hadRefreshFailure = appState.lastRefreshFailed
    const refreshButton = document.querySelector("#refresh-button")
    if (refreshButton) {
        refreshButton.disabled = true
        if (!background) {
            refreshButton.textContent = "Refreshing..."
        }
    }
    setSyncState("Refreshing...", "")

    const [countsOK, jobsOK] = await Promise.all([refreshCounts(), refreshJobs()])
    appState.refreshing = false

    if (refreshButton) {
        refreshButton.disabled = false
        refreshButton.textContent = "Refresh"
    }

    if (jobsOK === null) {
        // A newer state request owns the visible result and status.
    } else if (countsOK && jobsOK) {
        appState.lastUpdatedAt = new Date()
        appState.lastRefreshFailed = false
        setJobsAlert("")
        updateSyncStatusText()
        if (!background || hadRefreshFailure) {
            announceSync("Dashboard updated.")
        }
        document.querySelector("#sync-dot")?.classList.add("success")
        document.querySelector("#sync-dot")?.classList.remove("error")
    } else {
        appState.lastRefreshFailed = true
        setSyncState("Update failed · showing last available data", "error")
        if (!background || !hadRefreshFailure) {
            announceSync("Dashboard update failed. Showing the last available data.")
        }
        setJobsAlert(appState.jobsLoaded
            ? "Could not refresh all data. Showing the last successful result."
            : "Could not load jobs. Check the server and try again.")
    }

    if (appState.refreshQueued) {
        appState.refreshQueued = false
        void refreshAll({background: true})
    }

    return countsOK && jobsOK
}

async function refreshCounts() {
    const results = await Promise.all(JOB_TYPES.map(updateCount))
    return results.every(Boolean)
}

async function updateCount(jobType) {
    const card = document.querySelector(`[data-job-filter="${jobType}"]`)
    if (!card) {
        return true
    }
    const value = card.querySelector(".metric-value")

    try {
        const response = await fetch(`api/count/${jobType}`, {headers: {Accept: "application/json"}})
        if (!response.ok) {
            throw new Error(`Could not load ${jobType} count.`)
        }
        const body = await response.json()
        value.textContent = new Intl.NumberFormat().format(body.data)
        card.classList.remove("loading", "error")
        card.setAttribute("aria-label", `${body.data} ${jobType} jobs`)
        return true
    } catch (error) {
        if (value.textContent === "-") {
            value.textContent = "!"
        }
        card.classList.remove("loading")
        card.classList.add("error")
        card.setAttribute("aria-label", `${formatJobType(jobType)} count unavailable`)
        return false
    }
}

async function refreshJobs() {
    const requestID = ++appState.requestID
    const requestedJobType = appState.jobType
    appState.jobsLoading = true
    appState.hasMore = false
    renderPagination()

    if (!appState.jobsLoaded) {
        renderLoadingMessage()
    }

    const params = new URLSearchParams({
        limit: String(appState.limit),
        offset: String(appState.offset),
    })

    if (appState.capabilities.jobQuery) {
        if (appState.q) {
            params.set("q", appState.q)
        }
        if (appState.queue) {
            params.set("queue", appState.queue)
        }
        if (requestedJobType === "enqueued" && !appState.queue) {
            params.set("allQueues", "true")
        }
    } else if (requestedJobType === "enqueued" && appState.queue) {
        params.set("queue", appState.queue)
    }

    try {
        const response = await fetch(`api/jobs/${requestedJobType}?${params}`, {
            headers: {Accept: "application/json"},
        })
        if (!response.ok) {
            throw new Error(await readApiError(response, `Could not load ${requestedJobType} jobs.`))
        }
        const body = await response.json()

        if (requestID !== appState.requestID || requestedJobType !== appState.jobType) {
            return null
        }

        const nextJobs = Array.isArray(body.data) ? body.data : []
        const shouldRenderJobs = !appState.jobsLoaded || !sameJSON(appState.jobs, nextJobs)
        const focusedJobID = document.activeElement?.closest?.("[data-job-id]")?.dataset.jobId || null
        appState.jobs = nextJobs
        appState.hasMore = body.meta?.hasMore === true
        appState.jobsLoaded = true
        appState.jobsLoading = false
        setJobsAlert("")
        if (shouldRenderJobs) {
            renderJobs()
            restoreJobFocus(focusedJobID)
        } else {
            updateResultSummary()
            updateRuntimeValues()
        }
        renderPagination()

        if (appState.selectedJob && appState.selectedJobType === appState.jobType) {
            const previousJob = appState.selectedJob
            const refreshedJob = appState.jobs.find(job => String(job.id) === String(appState.selectedJob.id))
            if (refreshedJob) {
                appState.selectedJob = refreshedJob
                appState.detailTrigger = findJobTrigger(refreshedJob.id) || appState.detailTrigger
                if (!sameJSON(previousJob, refreshedJob)) {
                    renderJobDetailsPreservingState()
                }
            } else {
                closeStaleJobDetails()
            }
        }
        return true
    } catch (error) {
        if (requestID !== appState.requestID) {
            return null
        }
        appState.jobsLoading = false
        appState.hasMore = false
        if (!appState.jobsLoaded) {
            renderMessage(error.message || "Could not load jobs.")
        }
        renderPagination()
        return false
    }
}

function renderJobs() {
    const tableBody = document.querySelector("#jobs-table-body")
    const tableHead = document.querySelector("#jobs-table-head")
    const columns = getJobColumns(appState.jobType)

    tableHead.innerHTML = columns
        .map(column => `<th scope="col">${escapeHtml(column.label)}</th>`)
        .join("")

    if (appState.jobs.length === 0) {
        const hasFilters = Boolean(appState.q || appState.queue)
        renderMessage(hasFilters ? "No jobs match these filters." : "No jobs in this state.")
        updateResultSummary()
        return
    }

    tableBody.innerHTML = appState.jobs.map(job => `
        <tr>
            ${columns.map(column => {
                const classes = column.muted ? "muted-cell" : ""
                const className = classes ? ` class="${classes}"` : ""
                return `<td${className} data-label="${escapeHtml(column.label)}">${column.value(job)}</td>`
            }).join("")}
        </tr>
    `).join("")

    updateResultSummary()
    updateRuntimeValues()
}

function findJobTrigger(jobID) {
    return [...document.querySelectorAll("#jobs-table-body [data-job-id]")]
        .find(element => String(element.dataset.jobId) === String(jobID)) || null
}

function restoreJobFocus(jobID) {
    if (!jobID) {
        return
    }
    const trigger = findJobTrigger(jobID)
    if (trigger) {
        trigger.focus({preventScroll: true})
    } else {
        document.querySelector("#selected-state-title")?.focus({preventScroll: true})
    }
}

function renderLoadingMessage() {
    renderJobHeader()
    renderMessage(`Loading ${appState.jobType} jobs...`)
    document.querySelector("#result-summary").textContent = ""
    renderPagination()
}

function renderJobHeader() {
    const tableHead = document.querySelector("#jobs-table-head")
    const columns = getJobColumns(appState.jobType)
    tableHead.innerHTML = columns
        .map(column => `<th scope="col">${escapeHtml(column.label)}</th>`)
        .join("")
}

function renderMessage(message) {
    const tableBody = document.querySelector("#jobs-table-body")
    tableBody.innerHTML = `<tr class="message-row"><td colspan="${getJobColumns(appState.jobType).length}">${escapeHtml(message)}</td></tr>`
}

function renderPagination() {
    const previous = document.querySelector("#previous-page")
    const next = document.querySelector("#next-page")
    const summary = document.querySelector("#page-summary")
    const page = Math.floor(appState.offset / appState.limit) + 1

    previous.disabled = appState.jobsLoading || appState.offset === 0
    next.disabled = appState.jobsLoading || !appState.hasMore
    summary.textContent = `Page ${page}`
}

function updateResultSummary() {
    const summary = document.querySelector("#result-summary")
    if (!summary) {
        return
    }
    let message
    if (appState.jobs.length === 0) {
        message = "0 jobs"
    } else {
        const start = appState.offset + 1
        const end = appState.offset + appState.jobs.length
        message = `Showing ${start}–${end}`
    }
    if (summary.textContent !== message) {
        summary.textContent = message
    }
}

function selectJobType(jobType, {focusHeading = false} = {}) {
    if (!JOB_TYPES.includes(jobType)) {
        return
    }
    if (jobType !== appState.jobType) {
        closeOpenJobDetails()
    }
    appState.jobType = jobType
    normalizeFiltersForCapabilities()
    appState.offset = 0
    appState.jobsLoaded = false
    appState.jobsLoading = true
    appState.hasMore = false
    appState.jobs = []
    pushUrlState()
    updateSelectedState()
    applyCapabilityState()
    renderLoadingMessage()
    refreshJobs().then(ok => {
        if (ok === null) {
            return
        }
        if (!ok) {
            appState.lastRefreshFailed = true
            setSyncState("Update failed · showing last available data", "error")
            announceSync(`${formatJobType(appState.jobType)} jobs could not be updated.`)
            return
        }
        appState.lastRefreshFailed = false
        appState.lastUpdatedAt = new Date()
        updateSyncStatusText()
        announceSync(`${formatJobType(appState.jobType)} jobs updated.`)
    })

    if (focusHeading) {
        document.querySelector("#selected-state-title")?.focus({preventScroll: false})
    }
}

function loadSelectedState({focusHeading = false} = {}) {
    appState.jobsLoaded = false
    appState.jobsLoading = true
    appState.hasMore = false
    appState.jobs = []
    updateSelectedState()
    applyCapabilityState()
    renderLoadingMessage()
    refreshJobs().then(ok => {
        if (ok === null) {
            return
        }
        if (!ok) {
            appState.lastRefreshFailed = true
            setSyncState("Update failed · showing last available data", "error")
            announceSync(`${formatJobType(appState.jobType)} jobs could not be updated.`)
            return
        }
        appState.lastRefreshFailed = false
        appState.lastUpdatedAt = new Date()
        updateSyncStatusText()
        announceSync(`${formatJobType(appState.jobType)} jobs updated.`)
    })
    if (focusHeading) {
        document.querySelector("#selected-state-title")?.focus({preventScroll: false})
    }
}

function updateSelectedState() {
    const label = formatJobType(appState.jobType)
    const title = document.querySelector("#selected-state-title")
    const caption = document.querySelector("#jobs-table-caption")
    if (title) {
        title.textContent = `${label} jobs`
    }
    if (caption) {
        caption.textContent = `${label} jobs`
    }

    document.querySelectorAll("[data-job-filter]").forEach(link => {
        const active = link.dataset.jobFilter === appState.jobType
        link.classList.toggle("active", active)
        if (active) {
            link.setAttribute("aria-current", "page")
        } else {
            link.removeAttribute("aria-current")
        }
    })
}

function openJobDetails(jobID, trigger) {
    const job = appState.jobs.find(item => String(item.id) === String(jobID))
    if (!job) {
        return
    }
    hideRetryConfirmation({restoreFocus: false})
    appState.selectedJob = job
    appState.selectedJobType = appState.jobType
    appState.detailTrigger = trigger
    renderJobDetails()
    bootstrap.Offcanvas.getOrCreateInstance(document.querySelector("#job-details")).show()
}

function renderJobDetails() {
    const job = appState.selectedJob
    if (!job) {
        return
    }

    const title = document.querySelector("#job-details-title")
    const body = document.querySelector("#job-details-body")
    const retryButton = document.querySelector("#retry-job-button")
    const readOnlyHint = document.querySelector("#read-only-hint")
    const error = appState.selectedJobType === "failed" ? getErrorParts(job) : null
    const payload = prettyJSON(Object.hasOwn(job, "payload") ? job.payload : null)
    const retryDetail = Object.hasOwn(job, "retryCount")
        ? renderDetail("Retries", job.retryCount)
        : ""

    title.textContent = job.name || "Unnamed job"
    body.innerHTML = `
        <div class="detail-summary">
            <span class="state-badge ${appState.selectedJobType === "failed" ? "failed" : ""}">${escapeHtml(formatJobType(appState.selectedJobType))}</span>
            <span class="muted-cell">${escapeHtml(job.queue || "No queue")}</span>
        </div>
        <dl class="detail-grid">
            ${renderDetail("Job ID", job.id, true)}
            ${renderDetail("Queue", job.queue)}
            ${retryDetail}
            ${renderTimestampDetails(job)}
        </dl>
        <section class="detail-section" aria-labelledby="payload-heading">
            <div class="detail-section-header">
                <h3 id="payload-heading">Payload</h3>
                <button class="btn btn-outline-dark copy-button" type="button" data-copy="payload">Copy payload</button>
            </div>
            <pre class="code-block">${escapeHtml(payload)}</pre>
        </section>
        ${error ? renderErrorDetails(error) : ""}
    `

    const canRetry = appState.selectedJobType === "failed"
        && !appState.capabilities.readOnly
        && appState.capabilities.retryFailed
    retryButton.hidden = !canRetry
    readOnlyHint.hidden = canRetry
    if (!canRetry) {
        readOnlyHint.textContent = appState.selectedJobType === "failed"
            ? "Retry actions are disabled for this dashboard."
            : "No actions are available for this job state."
    }
}

function renderJobDetailsPreservingState() {
    const details = document.querySelector("#job-details")
    const stacktrace = details?.querySelector(".stacktrace-details")
    const stacktraceOpen = stacktrace?.open === true
    const activeCopyKind = document.activeElement?.closest?.("[data-copy]")?.dataset.copy || null
    const summaryFocused = document.activeElement === stacktrace?.querySelector("summary")

    renderJobDetails()

    const refreshedStacktrace = details?.querySelector(".stacktrace-details")
    if (stacktraceOpen && refreshedStacktrace) {
        refreshedStacktrace.open = true
    }
    if (activeCopyKind) {
        details?.querySelector(`[data-copy="${activeCopyKind}"]`)?.focus({preventScroll: true})
    } else if (summaryFocused) {
        refreshedStacktrace?.querySelector("summary")?.focus({preventScroll: true})
    }
}

function closeOpenJobDetails({stale = false} = {}) {
    if (!appState.selectedJob) {
        return
    }
    const details = document.querySelector("#job-details")
    const visible = details?.classList.contains("show") || details?.classList.contains("showing")
    appState.detailTrigger = null
    appState.detailCloseFocus = document.querySelector("#selected-state-title")
    if (visible) {
        bootstrap.Offcanvas.getOrCreateInstance(details).hide()
        if (stale) {
            showToast("This job is no longer in the current results.")
        }
    } else {
        appState.selectedJob = null
        appState.selectedJobType = null
    }
}

function closeStaleJobDetails() {
    closeOpenJobDetails({stale: true})
}

function renderDetail(label, value, wide = false) {
    if (value === undefined || value === null || value === "") {
        return ""
    }
    return `<div${wide ? " class=\"detail-wide\"" : ""}><dt>${escapeHtml(label)}</dt><dd>${escapeHtml(value)}</dd></div>`
}

function renderTimestampDetails(job) {
    const fields = [
        ["Scheduled", job.scheduledAt],
        ["Enqueue target", job.enqueueAt],
        ["Enqueued", job.enqueuedAt],
        ["Started", job.startedAt],
        ["Completed", job.completedAt],
        ["Last retry", job.lastRetryAt],
        ["Next retry", job.nextRetryAt],
    ]
    return fields
        .filter(([, value]) => value)
        .map(([label, value]) => renderDetail(label, formatTime(value)))
        .join("")
}

function renderErrorDetails(error) {
    return `
        <section class="detail-section" aria-labelledby="error-heading">
            <div class="detail-section-header">
                <h3 id="error-heading">Error</h3>
                <button class="btn btn-outline-dark copy-button" type="button" data-copy="error">Copy error</button>
            </div>
            <p class="error-message-block">${escapeHtml(error.message)}</p>
            ${error.stacktrace ? `
                <details class="stacktrace-details">
                    <summary>Stack trace</summary>
                    <pre class="code-block">${escapeHtml(error.stacktrace)}</pre>
                </details>
            ` : ""}
        </section>
    `
}

async function copyJobValue(kind) {
    const job = appState.selectedJob
    if (!job) {
        return
    }
    const value = kind === "payload"
        ? prettyJSON(Object.hasOwn(job, "payload") ? job.payload : null)
        : formatErrorForCopy(getErrorParts(job))
    try {
        await copyText(value)
        showToast(kind === "payload" ? "Payload copied." : "Error copied.")
    } catch (error) {
        showToast("Could not copy to the clipboard.")
    }
}

function showRetryConfirmation() {
    const job = appState.selectedJob
    if (!job
        || appState.selectedJobType !== "failed"
        || appState.capabilities.readOnly
        || !appState.capabilities.retryFailed) {
        return
    }
    const prompt = document.querySelector("#retry-prompt")
    prompt.textContent = `Retry ${job.name || "this job"} (${job.id}) with the same payload?`
    document.querySelector("#details-footer-default").hidden = true
    document.querySelector("#retry-confirmation").hidden = false
    document.querySelector("#confirm-retry")?.focus()
}

function hideRetryConfirmation({restoreFocus = true} = {}) {
    const defaultFooter = document.querySelector("#details-footer-default")
    const confirmation = document.querySelector("#retry-confirmation")
    if (!defaultFooter || !confirmation) {
        return
    }
    defaultFooter.hidden = false
    confirmation.hidden = true
    if (restoreFocus) {
        document.querySelector("#retry-job-button")?.focus()
    }
}

async function retrySelectedJob() {
    const job = appState.selectedJob
    const confirmButton = document.querySelector("#confirm-retry")
    if (!job
        || appState.selectedJobType !== "failed"
        || appState.capabilities.readOnly
        || !appState.capabilities.retryFailed) {
        return
    }

    confirmButton.disabled = true
    confirmButton.textContent = "Retrying..."
    try {
        const response = await fetch(`api/jobs/failed/${encodeURIComponent(job.id)}/retry`, {
            method: "POST",
            headers: {
                Accept: "application/json",
                "Content-Type": "application/json",
            },
            body: "{}",
        })
        if (!response.ok) {
            const message = await readApiError(response, "Could not retry this job.")
            if (response.status === 403) {
                await refreshCapabilities()
                normalizeFiltersForCapabilities({replaceUrl: true})
                applyCapabilityState()
                hideRetryConfirmation({restoreFocus: false})
                renderJobDetails()
                document.querySelector("#job-details .btn-close")?.focus({preventScroll: true})
            }
            throw new Error(message)
        }

        const body = await response.json()
        const retriedJob = body?.data?.job
        if (!retriedJob?.id || !retriedJob?.queue) {
            throw new Error("The server returned an invalid retry response.")
        }

        hideRetryConfirmation({restoreFocus: false})
        appState.detailTrigger = null
        appState.detailCloseFocus = document.querySelector("#toast-action")
        bootstrap.Offcanvas.getOrCreateInstance(document.querySelector("#job-details")).hide()
        showToast("Job placed back on its queue.", "View enqueued", buildStateHref("enqueued", {queue: retriedJob.queue}))
        appState.selectedJob = null
        appState.selectedJobType = null
        await refreshAll()
    } catch (error) {
        showToast(error.message || "Could not retry this job.")
        await refreshAll({background: true})
    } finally {
        confirmButton.disabled = false
        confirmButton.textContent = "Retry now"
    }
}

function showToast(message, actionLabel = "", actionHref = "") {
    const toastElement = document.querySelector("#app-toast")
    const messageElement = document.querySelector("#toast-message")
    const action = document.querySelector("#toast-action")
    messageElement.textContent = message
    action.hidden = !actionLabel
    action.textContent = actionLabel
    action.href = actionHref || "#"
    bootstrap.Toast.getOrCreateInstance(toastElement, {delay: 5_000}).show()
}

function getErrorParts(job) {
    const raw = String(job.error || "-")
    try {
        const parsed = JSON.parse(raw)
        return {
            message: String(parsed.message || "-"),
            stacktrace: String(parsed.stacktrace || ""),
        }
    } catch (error) {
        return {message: raw, stacktrace: ""}
    }
}

function formatErrorForCopy(error) {
    return error.stacktrace ? `${error.message}\n\n${error.stacktrace}` : error.message
}

function renderJobName(job) {
    const label = job.name || "Unnamed job"
    return `<span class="job-name">${escapeHtml(label)}</span>`
}

function renderDetailsAction(job) {
    const label = job.name || "Unnamed job"
    return `<button class="btn btn-outline-dark view-job-button" type="button" data-job-id="${escapeHtml(job.id)}" aria-label="View details for ${escapeHtml(label)}, job ${escapeHtml(job.id)}">View</button>`
}

function getJobColumns(jobType) {
    return JOB_COLUMNS[jobType] || JOB_COLUMNS.processing
}

function updateRuntimeValues() {
    document.querySelectorAll("[data-runtime-start]").forEach(element => {
        element.textContent = formatRuntime(element.dataset.runtimeStart)
    })
}

function setJobsAlert(message) {
    const alert = document.querySelector("#jobs-alert")
    if (!alert) {
        return
    }
    const hidden = !message
    if (alert.hidden === hidden && alert.textContent === message) {
        return
    }
    alert.hidden = hidden
    if (alert.textContent !== message) {
        alert.textContent = message
    }
}

function setSyncState(message, type) {
    const status = document.querySelector("#sync-status")
    const dot = document.querySelector("#sync-dot")
    if (status) {
        status.textContent = message
    }
    if (dot) {
        dot.classList.toggle("success", type === "success")
        dot.classList.toggle("error", type === "error")
    }
}

function announceSync(message) {
    const announcement = document.querySelector("#sync-announcement")
    if (!announcement) {
        return
    }
    announcement.textContent = ""
    window.setTimeout(() => {
        announcement.textContent = message
    }, 0)
}

async function copyText(value) {
    if (navigator.clipboard?.writeText) {
        try {
            await navigator.clipboard.writeText(value)
            return
        } catch (error) {
            // Fall back when clipboard permission or the API is unavailable.
        }
    }

    const previousFocus = document.activeElement
    const textarea = document.createElement("textarea")
    textarea.value = value
    textarea.setAttribute("readonly", "")
    textarea.style.position = "fixed"
    textarea.style.opacity = "0"
    document.body.appendChild(textarea)
    textarea.select()
    const copied = document.execCommand("copy")
    textarea.remove()
    if (previousFocus?.isConnected) {
        previousFocus.focus({preventScroll: true})
    }
    if (!copied) {
        throw new Error("Clipboard copy failed.")
    }
}

function updateSyncStatusText() {
    if (!appState.lastUpdatedAt || appState.lastRefreshFailed || appState.refreshing) {
        return
    }
    const elapsedSeconds = Math.max(0, Math.floor((Date.now() - appState.lastUpdatedAt.getTime()) / 1_000))
    const message = elapsedSeconds < 5
        ? "Updated just now"
        : `Updated ${elapsedSeconds}s ago`
    setSyncState(message, "success")
}

function populateControlsFromState() {
    const search = document.querySelector("#job-search")
    const queue = document.querySelector("#queue-filter")
    const pageSize = document.querySelector("#page-size")
    if (search) {
        search.value = appState.q
    }
    if (queue) {
        queue.value = appState.queue
    }
    if (pageSize) {
        pageSize.value = String(appState.limit)
    }
}

function normalizeFiltersForCapabilities({replaceUrl = false} = {}) {
    let changed = false
    if (!appState.capabilities.jobQuery && appState.q) {
        appState.q = ""
        changed = true
    }
    if (!appState.capabilities.jobQuery && appState.jobType !== "enqueued" && appState.queue) {
        appState.queue = ""
        changed = true
    }
    if (changed) {
        appState.offset = 0
        populateControlsFromState()
        if (replaceUrl) {
            pushUrlState({replace: true})
        }
    }
    return changed
}

function readUrlState() {
    const params = new URLSearchParams(window.location.search)
    const jobType = params.get("jobType")
    const rawOffset = Number.parseInt(params.get("offset") || "0", 10)
    return {
        jobType: JOB_TYPES.includes(jobType) ? jobType : "processing",
        q: params.get("q") || "",
        queue: params.get("queue") || "",
        limit: parsePageSize(params.get("limit")),
        offset: Number.isFinite(rawOffset) && rawOffset > 0 ? rawOffset : 0,
    }
}

function parsePageSize(value) {
    const parsed = Number.parseInt(value || "25", 10)
    return PAGE_SIZES.includes(parsed) ? parsed : 25
}

function pushUrlState({replace = false} = {}) {
    const url = new URL(window.location.href)
    url.search = ""
    url.searchParams.set("jobType", appState.jobType)
    if (appState.q) {
        url.searchParams.set("q", appState.q)
    }
    if (appState.queue) {
        url.searchParams.set("queue", appState.queue)
    }
    if (appState.limit !== 25) {
        url.searchParams.set("limit", String(appState.limit))
    }
    if (appState.offset > 0) {
        url.searchParams.set("offset", String(appState.offset))
    }
    window.history[replace ? "replaceState" : "pushState"]({}, "", url)
}

function buildStateHref(jobType, {queue = ""} = {}) {
    const url = new URL(window.location.href)
    url.search = ""
    url.searchParams.set("jobType", jobType)
    if (queue) {
        url.searchParams.set("queue", queue)
    }
    return url.toString()
}

async function readApiError(response, fallback) {
    try {
        const body = await response.json()
        return body.error || fallback
    } catch (error) {
        return fallback
    }
}

function formatJobType(jobType) {
    return jobType.charAt(0).toUpperCase() + jobType.slice(1)
}

function formatTime(value) {
    if (!value) {
        return "-"
    }
    const date = new Date(value)
    if (Number.isNaN(date.getTime())) {
        return "-"
    }
    return new Intl.DateTimeFormat(undefined, {
        dateStyle: "medium",
        timeStyle: "short",
    }).format(date)
}

function formatRuntime(startedAt, endedAt = new Date().toISOString()) {
    if (!startedAt) {
        return "-"
    }
    const started = new Date(startedAt)
    const ended = new Date(endedAt)
    if (Number.isNaN(started.getTime()) || Number.isNaN(ended.getTime())) {
        return "-"
    }
    const totalSeconds = Math.max(0, Math.floor((ended.getTime() - started.getTime()) / 1_000))
    const hours = Math.floor(totalSeconds / 3_600)
    const minutes = Math.floor((totalSeconds % 3_600) / 60)
    const seconds = totalSeconds % 60
    if (hours > 0) {
        return `${hours}h ${minutes}m`
    }
    if (minutes > 0) {
        return `${minutes}m ${seconds}s`
    }
    return `${seconds}s`
}

function prettyJSON(value) {
    try {
        return JSON.stringify(value, null, 2)
    } catch (error) {
        return String(value)
    }
}

function sameJSON(first, second) {
    try {
        return JSON.stringify(first) === JSON.stringify(second)
    } catch (error) {
        return false
    }
}

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;")
}
