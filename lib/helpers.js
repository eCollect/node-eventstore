module.exports = {
	async: (debug) => ({
		resolvify: (callback) => callback ? callback.bind(null, null) : null,
		rejectify: (callback) => (err) => {
			if (debug) debug(err);
			if (callback) callback(err);
		},
	}),
};
