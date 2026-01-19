package io.github.alexoooo.sample.async;

public enum AsyncState {
    Created,
    Starting,
    Running,
    Closing,

    /**
     * The worker is stopped, either due to:
     *      failure,
     *      running out of work,
     *      or being closed (and finishing any pending work).
     * Note that stopping doesn't mean that all output from the worker is consumed,
     *  simply that there is no more work being actively performed.
     */
    Terminal
}
