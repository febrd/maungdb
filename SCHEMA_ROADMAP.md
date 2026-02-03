Command expectation:

maung schema create pamake id,ngaran,umur \
  --read=user,admin,supermaung \
  --write=admin,supermaung


aturan:

Hanya admin & supermaung boleh bikin schema.

Tidak perlu ubah config, enforcement di CLI.