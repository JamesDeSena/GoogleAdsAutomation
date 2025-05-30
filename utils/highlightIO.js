const { H } = require('@highlight-run/node');

H.init({
	projectID: `${process.env.HIGHLIGHTIO_PROJECTID}`,
	serviceName: `${process.env.HIGHLIGHTIO_SERVICENAME}`,
	environment: 'production',
})

const highlightErrorHandler = (err, req, res, next) => {
    if (res.headersSent) {
        return next(err);
    }

    const parsed = H.parseHeaders(req.headers);
    H.consumeError(err, parsed.secureSessionId, parsed.requestId);

    next(err);
};

const highlightJobErrorHandler = (error, context = {}) => {
    H.consumeError(error, context.secureSessionId, context.requestId, context);
};

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    highlightJobErrorHandler(reason, { type: 'unhandledRejection' });
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
    highlightJobErrorHandler(error, { type: 'uncaughtException' });
});

module.exports = {
  highlightErrorHandler,
  highlightJobErrorHandler,
};