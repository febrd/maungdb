const API = {
    async post(url, data) {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data)
      });
      return res.json();
    },
  
    async get(url) {
      const res = await fetch(url);
      return res.json();
    }
  };
  