const JOB_TYPES = ["scheduled", "enqueued", "processing", "processed", "failed"]
const JOB_COLUMNS = {
    scheduled: [
        {label: "Name", value: job => escapeHtml(job.name || "-")},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Scheduled at", value: job => formatTime(job.scheduledAt), muted: true},
        {label: "Scheduled for", value: job => formatTime(job.enqueueAt), muted: true},
    ],
    enqueued: [
        {label: "Name", value: job => escapeHtml(job.name || "-")},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Retries", value: job => job.retryCount || 0},
        {label: "Enqueued at", value: job => formatTime(job.enqueuedAt), muted: true},
    ],
    processing: [
        {label: "Name", value: job => escapeHtml(job.name || "-")},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Started at", value: job => formatTime(job.startedAt), muted: true},
        {label: "Runtime", value: job => formatRuntime(job.startedAt), muted: true},
    ],
    processed: [
        {label: "Name", value: job => escapeHtml(job.name || "-")},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Started at", value: job => formatTime(job.startedAt), muted: true},
        {label: "Runtime", value: job => formatRuntime(job.startedAt, job.completedAt), muted: true},
    ],
    failed: [
        {label: "Name", value: job => escapeHtml(job.name || "-")},
        {label: "Queue", value: job => escapeHtml(job.queue || "-"), muted: true},
        {label: "Retries", value: job => job.retryCount || 0},
        {label: "Next retry at", value: job => formatTime(job.nextRetryAt), muted: true},
    ],
}

document.addEventListener("DOMContentLoaded", () => {
    const selectedJobType = getSelectedJobType()

    document.querySelectorAll(".nav-link").forEach(navLink => {
        const url = new URL(navLink.href)
        if (url.searchParams.get("jobType") === selectedJobType) {
            navLink.classList.add("active")
        }
    })

    document.querySelectorAll("[data-job-filter]").forEach(link => {
        if (link.dataset.jobFilter === selectedJobType) {
            link.classList.add("active")
        }
    })

    updateSelectedState(selectedJobType)
    refreshCounts()
    refreshJobs(selectedJobType)

    const queueFilter = document.querySelector("#queue-filter")
    if (queueFilter) {
        queueFilter.addEventListener("change", () => refreshJobs(getSelectedJobType()))
    }
})

const getSelectedJobType = () => {
    const params = new URLSearchParams(window.location.search)
    const jobType = params.get("jobType")

    return JOB_TYPES.includes(jobType) ? jobType : "processing"
}

const updateSelectedState = (jobType) => {
    const title = document.querySelector("#selected-state-title")
    const queueFilter = document.querySelector(".queue-filter")
    if (!title) {
        return
    }

    const label = formatJobType(jobType)
    title.textContent = `${label} jobs`

    if (queueFilter) {
        queueFilter.hidden = jobType !== "enqueued"
    }
}

const refreshCounts = async () => {
    await Promise.all(JOB_TYPES.map(updateCount))
}

const updateCount = async (jobType) => {
    const card = document.querySelector(`[data-job-type="${jobType}"]`)
    if (!card) {
        return
    }

    const value = card.querySelector(".metric-value")

    try {
        const res = await fetch(`/api/count/${jobType}`)
        if (!res.ok) {
            throw new Error(`failed to fetch ${jobType} count`)
        }

        const body = await res.json()
        value.textContent = new Intl.NumberFormat().format(body.data)
        card.classList.remove("loading")
    } catch (err) {
        value.textContent = "!"
        card.classList.remove("loading")
        card.classList.add("error")
    }
}

const formatJobType = (jobType) => {
    return jobType.charAt(0).toUpperCase() + jobType.slice(1)
}

const refreshJobs = async (jobType) => {
    const tableBody = document.querySelector("#jobs-table-body")
    if (!tableBody) {
        return
    }

    renderJobHeader(jobType)
    tableBody.innerHTML = renderMessageRow(jobType, `Loading ${jobType} jobs...`)

    try {
        const queue = document.querySelector("#queue-filter")?.value || "default"
        const params = new URLSearchParams({limit: "25"})
        if (jobType === "enqueued") {
            params.set("queue", queue)
        }

        const res = await fetch(`/api/jobs/${jobType}?${params}`)
        if (!res.ok) {
            throw new Error(`failed to fetch ${jobType} jobs`)
        }

        const body = await res.json()
        renderJobs(jobType, body.data || [])
    } catch (err) {
        tableBody.innerHTML = renderMessageRow(jobType, "Could not load jobs.")
    }
}

const renderJobHeader = (jobType) => {
    const tableHead = document.querySelector("#jobs-table-head")
    if (!tableHead) {
        return
    }

    tableHead.innerHTML = getJobColumns(jobType)
        .map(column => `<th>${escapeHtml(column.label)}</th>`)
        .join("")
}

const renderJobs = (jobType, jobs) => {
    const tableBody = document.querySelector("#jobs-table-body")
    if (jobs.length === 0) {
        tableBody.innerHTML = renderMessageRow(jobType, "No jobs in this state.")
        return
    }

    const columns = getJobColumns(jobType)
    tableBody.innerHTML = jobs.map(job => `
        <tr>
            ${columns.map(column => {
                const className = column.muted ? ` class="muted-cell"` : ""
                return `<td${className}>${column.value(job)}</td>`
            }).join("")}
        </tr>
    `).join("")
}

const getJobColumns = (jobType) => {
    return JOB_COLUMNS[jobType] || JOB_COLUMNS.processing
}

const renderMessageRow = (jobType, message) => {
    return `<tr><td colspan="${getJobColumns(jobType).length}" class="muted-cell">${escapeHtml(message)}</td></tr>`
}

const formatTime = (value) => {
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

const formatRuntime = (startedAt, endedAt = new Date().toISOString()) => {
    if (!startedAt) {
        return "-"
    }

    const started = new Date(startedAt)
    const ended = new Date(endedAt)
    if (Number.isNaN(started.getTime()) || Number.isNaN(ended.getTime())) {
        return "-"
    }

    const totalSeconds = Math.max(0, Math.floor((ended.getTime() - started.getTime()) / 1000))
    const hours = Math.floor(totalSeconds / 3600)
    const minutes = Math.floor((totalSeconds % 3600) / 60)
    const seconds = totalSeconds % 60

    if (hours > 0) {
        return `${hours}h ${minutes}m`
    }

    if (minutes > 0) {
        return `${minutes}m ${seconds}s`
    }

    return `${seconds}s`
}

const escapeHtml = (value) => {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;")
}
